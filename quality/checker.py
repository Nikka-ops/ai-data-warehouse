# -*- coding: utf-8 -*-
"""
AI 实时质检器

职责边界很窄，只干一件事：

    读 Flink 已经算好的窗口 → 规则判异常 → 命中才调 LLM → 写告警

它不做任何数据加工。窗口该在什么时候触发、迟到数据怎么处理、
故障后怎么恢复，这些全部是 Flink 的事（watermark 决定窗口何时可以
安全触发，checkpoint 保证故障不丢数）。这里只消费结果。

这条边界很重要：用 Python 每分钟轮询去"算"窗口，必然会读到还没写完的
不完整窗口，算出来的 GMV 系统性偏小，而且一旦某轮异常，那一分钟的数据
就永远不会被补算。所以这里只查 window_end < NOW() 的已关闭窗口，
并且从告警表的断点往后补，而不是只看最新一分钟。

「规则先判、命中才调 LLM」：正常窗口 0 次 LLM 调用。
规则是毫秒级的，LLM 只负责给已经确定的异常补上原因分析和处置建议。

运行：
    python quality/checker.py                # 持续运行
    python quality/checker.py --once         # 只跑一轮
    python quality/checker.py --backfill 60  # 补算最近 60 分钟
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from openai import OpenAI

from common.doris_client import get_client


# ── 阈值配置 ──────────────────────────────────────────────────
# 集中放在这里，而不是散在判断逻辑里写死
SURGE_RATIO        = 3.0     # 订单量突增倍数
SLUMP_RATIO        = 0.2     # 订单量骤降到基线的比例
PRICE_RATIO        = 2.5     # 件均价异常倍数
ABNORMAL_RATE      = 0.05    # 异常价格订单占比阈值
PAY_CONV_MIN       = 0.50    # 支付转化率下限（分钟窗口内）
LATE_RATE          = 0.05    # 迟到数据占比阈值
LAG_SECONDS        = 120     # 端到端延迟阈值（秒）
MIN_BASELINE_CNT   = 10      # 基线订单数低于此值不做比例判断，避免小样本误报
BASELINE_MINUTES   = 60      # 基线回看窗口


def get_llm():
    return OpenAI(
        api_key=os.getenv('DEEPSEEK_API_KEY', ''),
        base_url=os.getenv('DEEPSEEK_API_BASE', 'https://api.deepseek.com'),
        timeout=30.0,
    )


def ai_enabled() -> bool:
    return bool(os.getenv('DEEPSEEK_API_KEY', '').strip())


# ── 取窗口 ────────────────────────────────────────────────────

def get_last_processed_window(doris) -> datetime | None:
    """
    上次处理到哪个窗口了。

    直接从告警表反查，不额外维护 offset 表：
    告警表本身就是这个脚本的输出，用它当断点是自洽的。
    """
    res = doris.query("""
        SELECT MAX(window_start)
        FROM stream.ai_quality_alerts
        WHERE alert_date >= CURDATE() - INTERVAL 2 DAY
    """)
    return res.first_row[0]


def get_pending_windows(doris, since: datetime | None, limit: int = 120) -> list[dict]:
    """
    取所有「Flink 已经算完、但这里还没质检过」的分钟窗口。

    关键是 window_end < NOW()：只处理已经关闭的窗口。
    Flink 的窗口要等 watermark 越过 window_end 才触发，
    此刻读到的就是完整结果，不会再变 —— 这正是原实现缺的那道保险。
    """
    where_since = ""
    params = []
    if since is not None:
        where_since = "AND t.window_start > %s"
        params.append(since)

    # 三个源拼成一个窗口画像：
    #   交易窗口   订单量、GMV、件均价、异常价格数、链路延迟
    #   支付窗口   同一分钟的支付笔数 —— 算支付转化率的分子
    #   迟到兜底表 同一分钟落到兜底表的条数 —— 算迟到占比
    # 三者用 window_start 对齐。支付和迟到都用 LEFT JOIN：
    # 那一分钟没人支付、没有迟到数据，是正常情况，不该让整行消失。
    sql = f"""
        SELECT
            t.window_start,
            t.window_end,
            t.order_cnt,
            t.order_user_cnt,
            t.sku_num,
            t.order_amount,
            CASE WHEN t.sku_num > 0
                 THEN ROUND(t.order_amount / t.sku_num, 2) ELSE 0 END AS avg_price,
            t.max_order_price,
            t.abnormal_price_cnt,
            t.max_lag_seconds,
            t.avg_lag_seconds,
            COALESCE(p.pay_cnt, 0)      AS pay_cnt,
            COALESCE(p.pay_amount, 0)   AS pay_amount,
            COALESCE(l.late_cnt, 0)     AS late_cnt
        FROM dws.trade_window_agg t
        LEFT JOIN (
            SELECT window_start, pay_cnt, pay_amount
            FROM dws.pay_window_agg
            WHERE window_type  = 'TUMBLE_1M'
              AND payment_type = 'ALL'
              AND stat_date   >= CURDATE() - INTERVAL 1 DAY
        ) p ON t.window_start = p.window_start
        LEFT JOIN (
            SELECT
                -- 用 DATE_TRUNC 而不是 DATE_FORMAT：这条 SQL 有时带参数、
                -- 有时不带，而 pymysql 只在带参数时才做 % 格式化 ——
                -- 格式串里的 % 会在无参数分支下原样留在 SQL 里，直接语法错误。
                DATE_TRUNC(event_time, 'minute') AS w,
                COUNT(*) AS late_cnt
            FROM stream.late_records
            WHERE late_date >= CURDATE() - INTERVAL 1 DAY
            GROUP BY 1
        ) l ON t.window_start = l.w
        WHERE t.window_type    = 'TUMBLE_1M'
          AND t.category1_name = 'ALL'
          AND t.region_name    = 'ALL'
          AND t.stat_date     >= CURDATE() - INTERVAL 1 DAY
          AND t.window_end     < NOW()
          {where_since}
        ORDER BY t.window_start
        LIMIT {int(limit)}
    """
    res = doris.query(sql, params or None)

    cols = ['window_start', 'window_end', 'order_cnt', 'order_user_cnt', 'sku_num',
            'order_amount', 'avg_price', 'max_order_price', 'abnormal_price_cnt',
            'max_lag_seconds', 'avg_lag_seconds', 'pay_cnt', 'pay_amount', 'late_cnt']
    return [dict(zip(cols, row)) for row in res.result_rows]


def get_baseline(doris, window_start: datetime) -> dict:
    """
    取该窗口之前 N 分钟的基线。

    注意基线区间是「该窗口之前」而不是「现在之前」—— 补算历史窗口时，
    必须用当时的基线，不能用现在的，否则补出来的告警是错的。
    """
    res = doris.query("""
        SELECT
            AVG(order_cnt),
            AVG(order_amount),
            AVG(CASE WHEN sku_num > 0 THEN order_amount / sku_num ELSE NULL END),
            STDDEV_POP(order_cnt)
        FROM dws.trade_window_agg
        WHERE window_type    = 'TUMBLE_1M'
          AND category1_name = 'ALL'
          AND region_name    = 'ALL'
          AND window_start  <  %s
          AND window_start  >= %s
    """, (window_start, window_start - timedelta(minutes=BASELINE_MINUTES)))

    r = res.first_row
    return {
        'avg_order_cnt': float(r[0] or 0),
        'avg_gmv':       float(r[1] or 0),
        'avg_price':     float(r[2] or 0),
        'std_order_cnt': float(r[3] or 0),
    }


# ── 规则检测（毫秒级，不调 LLM）────────────────────────────────

def detect_by_rules(cur: dict, base: dict) -> list[dict]:
    """纯规则判断。返回命中的告警列表。"""
    alerts = []

    order_cnt = int(cur['order_cnt'] or 0)
    avg_price = float(cur['avg_price'] or 0)
    base_cnt  = base['avg_order_cnt']

    # 1. 订单量突增 / 骤降
    if base_cnt >= MIN_BASELINE_CNT:
        ratio = order_cnt / base_cnt
        if ratio > SURGE_RATIO:
            alerts.append({
                'alert_type': 'ANOMALY', 'severity': 'HIGH', 'field_name': 'order_cnt',
                'detail': f"订单量突增 {ratio:.1f}x：当前 {order_cnt} 单，基线 {base_cnt:.0f} 单",
                'metric_value': order_cnt,
                'baseline_value': base_cnt,
                'threshold_value': base_cnt * SURGE_RATIO,
            })
        elif ratio < SLUMP_RATIO:
            alerts.append({
                'alert_type': 'ANOMALY', 'severity': 'MEDIUM', 'field_name': 'order_cnt',
                'detail': f"订单量骤降至基线的 {ratio:.0%}：当前 {order_cnt} 单，基线 {base_cnt:.0f} 单",
                'metric_value': order_cnt,
                'baseline_value': base_cnt,
                'threshold_value': base_cnt * SLUMP_RATIO,
            })

    # 2. 件均价异常
    if avg_price > 0 and base['avg_price'] > 0:
        pr = avg_price / base['avg_price']
        if pr > PRICE_RATIO:
            alerts.append({
                'alert_type': 'QUALITY', 'severity': 'MEDIUM', 'field_name': 'avg_price',
                'detail': f"件均价异常偏高 {pr:.1f}x：当前 ￥{avg_price:.2f}，基线 ￥{base['avg_price']:.2f}",
                'metric_value': avg_price,
                'baseline_value': base['avg_price'],
                'threshold_value': base['avg_price'] * PRICE_RATIO,
            })

    # 3. 异常价格订单占比
    #    Flink 在 DWD 层就标了 is_price_abnormal（成交价显著偏离商品标价），
    #    这里只是把标记汇总成比例。占比突然抬头通常是配置错价或薅羊毛，
    #    比等运营发现要早很多。
    if order_cnt > 0:
        abn_rate = int(cur['abnormal_price_cnt'] or 0) / order_cnt
        if abn_rate > ABNORMAL_RATE:
            alerts.append({
                'alert_type': 'QUALITY', 'severity': 'HIGH', 'field_name': 'abnormal_price_cnt',
                'detail': f"异常价格订单占比 {abn_rate:.1%} 超阈值 {ABNORMAL_RATE:.0%}："
                          f"{cur['abnormal_price_cnt']}/{order_cnt} 单成交价显著偏离标价",
                'metric_value': abn_rate,
                'baseline_value': 0,
                'threshold_value': ABNORMAL_RATE,
            })

    # 4. 支付转化率骤降
    #    分子分母取的是同一分钟窗口，口径对齐。
    #    小样本不判：一分钟只有几单时，转化率天然抖得厉害，会全是误报。
    pay_cnt = int(cur['pay_cnt'] or 0)
    if order_cnt >= MIN_BASELINE_CNT:
        conv = pay_cnt / order_cnt
        if conv < PAY_CONV_MIN:
            alerts.append({
                'alert_type': 'ANOMALY', 'severity': 'HIGH', 'field_name': 'pay_cnt',
                'detail': f"支付转化率 {conv:.1%} 低于阈值 {PAY_CONV_MIN:.0%}："
                          f"{pay_cnt}/{order_cnt} 单完成支付，检查支付回调链路",
                'metric_value': conv,
                'baseline_value': PAY_CONV_MIN,
                'threshold_value': PAY_CONV_MIN,
            })

    # 5. 迟到数据占比
    #    迟到率飙高通常意味着上游延迟或 watermark 容忍度设得不合适。
    #    这些数据没有丢，都在 stream.late_records 里带着 Kafka 位点，
    #    但占比高说明窗口结果的完整性在下降，需要有人知道。
    if order_cnt > 0:
        late_rate = int(cur['late_cnt'] or 0) / order_cnt
        if late_rate > LATE_RATE:
            alerts.append({
                'alert_type': 'LATENCY', 'severity': 'MEDIUM', 'field_name': 'late_cnt',
                'detail': f"迟到数据占比 {late_rate:.1%} 超阈值 {LATE_RATE:.0%}："
                          f"{cur['late_cnt']}/{order_cnt} 条超出 watermark 容忍，已进兜底表",
                'metric_value': late_rate,
                'baseline_value': 0,
                'threshold_value': LATE_RATE,
            })

    # 6. 端到端延迟 —— 链路健康度
    max_lag = int(cur['max_lag_seconds'] or 0)
    if max_lag > LAG_SECONDS:
        alerts.append({
            'alert_type': 'LATENCY', 'severity': 'HIGH', 'field_name': 'max_lag_seconds',
            'detail': f"端到端延迟 {max_lag}s 超阈值 {LAG_SECONDS}s"
                      f"（平均 {cur['avg_lag_seconds']}s），检查 Flink 背压与 Kafka 积压",
            'metric_value': max_lag,
            'baseline_value': 0,
            'threshold_value': LAG_SECONDS,
        })

    return alerts


# ── AI 分析（仅在规则命中后调用）──────────────────────────────

def enrich_with_ai(alerts: list[dict], cur: dict, base: dict) -> None:
    """
    给已命中的告警补上 LLM 的原因分析和处置建议。原地修改 alerts。

    正常情况规则一条都不命中，这个函数根本不会被调用 —— 这是原项目
    "规则+AI 双重检测"设计里最值钱的部分，保留下来。
    """
    if not alerts:
        return

    if not ai_enabled():
        for a in alerts:
            a['ai_suggestion'] = '未配置 DEEPSEEK_API_KEY，仅规则告警'
            a['llm_called'] = 0
        return

    desc = "\n".join(f"{i+1}. {a['detail']}" for i, a in enumerate(alerts))
    prompt = f"""你是电商实时数据运营专家。实时监控发现以下异常：

窗口：{cur['window_start']} ~ {cur['window_end']}
当前：订单 {cur['order_cnt']} 单，GMV ￥{float(cur['order_amount'] or 0):.0f}，件均价 ￥{float(cur['avg_price'] or 0):.2f}，支付 {cur['pay_cnt']} 单，异常价格 {cur['abnormal_price_cnt']} 单，迟到 {cur['late_cnt']} 条，最大延迟 {cur['max_lag_seconds']}s
基线：订单均值 {base['avg_order_cnt']:.0f} 单，GMV 均值 ￥{base['avg_gmv']:.0f}

异常列表：
{desc}

请**严格按行**逐条回答，第 N 行对应第 N 个异常，不要写序号以外的任何前缀。
每行格式：可能原因 | 建议操作 | 紧急程度（3分钟内/10分钟内/1小时内）
每行不超过两句话，用中文。"""

    try:
        resp = get_llm().chat.completions.create(
            model=os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'),
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.3,
            max_tokens=500,
        )
        content = (resp.choices[0].message.content or '').strip()
        lines = [ln.strip() for ln in content.split('\n') if ln.strip()]

        # 原实现在这里有个错位 bug：AI 返回的行数少于告警数时，
        # 剩下的告警会被塞进「完整回答全文」，导致同一段文字重复出现在
        # 多条告警里。这里改成明确标注对不上，而不是硬塞。
        for i, a in enumerate(alerts):
            a['ai_suggestion'] = lines[i] if i < len(lines) else '（LLM 未针对该条给出建议）'
            a['llm_called'] = 1

    except Exception as e:
        for a in alerts:
            a['ai_suggestion'] = f'AI 分析不可用：{str(e)[:150]}'
            a['llm_called'] = 0


# ── 写告警 ────────────────────────────────────────────────────

def write_alerts(doris, alerts: list[dict], cur: dict) -> None:
    """
    写入 Doris。

    alert_key = 窗口 + 类型 + 字段，配合表上的 Unique Key 天然去重：
    补算同一个窗口时不会产生重复告警，重跑多少次结果都一样。
    """
    if not alerts:
        return

    ws = cur['window_start']
    rows = []
    for a in alerts:
        rows.append([
            ws.date() if hasattr(ws, 'date') else ws,
            f"{ws:%Y%m%d%H%M}_{a['alert_type']}_{a['field_name']}",
            datetime.now(),
            a['alert_type'],
            a['severity'],
            'dws.trade_window_agg',
            a['field_name'],
            a['detail'][:1000],
            (a.get('ai_suggestion') or '')[:2000],
            ws,
            cur['window_end'],
            float(a.get('metric_value') or 0),
            float(a.get('baseline_value') or 0),
            float(a.get('threshold_value') or 0),
            int(a.get('llm_called') or 0),
        ])

    doris.insert(
        'stream.ai_quality_alerts',
        rows,
        column_names=[
            'alert_date', 'alert_key', 'alert_time', 'alert_type', 'severity',
            'table_name', 'field_name', 'detail', 'ai_suggestion',
            'window_start', 'window_end', 'metric_value', 'baseline_value',
            'threshold_value', 'llm_called',
        ],
    )


# ── 主流程 ────────────────────────────────────────────────────

def process_once(doris, backfill_minutes: int = None) -> int:
    """
    处理一轮：把所有待质检的窗口补完。

    返回处理的窗口数。
    """
    if backfill_minutes:
        since = datetime.now() - timedelta(minutes=backfill_minutes)
        print(f"[补算] 回看最近 {backfill_minutes} 分钟")
    else:
        since = get_last_processed_window(doris)

    windows = get_pending_windows(doris, since)
    if not windows:
        print(f"[{datetime.now():%H:%M:%S}] 没有待质检的窗口")
        return 0

    print(f"[{datetime.now():%H:%M:%S}] 待质检窗口 {len(windows)} 个")

    processed = 0
    for cur in windows:
        # 单个窗口失败不能影响其他窗口 —— 原实现一个异常就整轮跳过
        try:
            base   = get_baseline(doris, cur['window_start'])
            alerts = detect_by_rules(cur, base)

            if alerts:
                enrich_with_ai(alerts, cur, base)
                write_alerts(doris, alerts, cur)
                print(f"  {cur['window_start']:%H:%M}  订单 {cur['order_cnt']:>5}  "
                      f"⚠ {len(alerts)} 条告警")
                for a in alerts:
                    print(f"       [{a['severity']:<6}] {a['detail']}")
            else:
                print(f"  {cur['window_start']:%H:%M}  订单 {cur['order_cnt']:>5}  正常")

            processed += 1

        except Exception as e:
            print(f"  {cur['window_start']:%H:%M}  处理失败：{e}")

    return processed


def run_continuous(interval: int = 60):
    print("=" * 62)
    print("  AI 实时质检器")
    print("  职责：读 Flink 窗口结果 → 规则判异常 → 命中才调 LLM → 写告警")
    print(f"  AI:   {'已启用' if ai_enabled() else '未配置 API Key，仅规则告警'}")
    print("=" * 62)

    doris = get_client()
    if not doris.ping():
        print("连不上 Doris，检查 DORIS_HOST / 服务是否启动")
        sys.exit(1)

    while True:
        try:
            process_once(doris)
        except KeyboardInterrupt:
            print("\n停止质检器")
            break
        except Exception as e:
            # 这里失败不再意味着丢一个窗口：下一轮会从告警表的断点
            # 重新往后补，漏掉的窗口自然被捡回来
            print(f"[ERROR] 本轮失败，下一轮会自动补算：{e}")

        time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AI 实时质检器')
    parser.add_argument('--once', action='store_true', help='只跑一轮')
    parser.add_argument('--backfill', type=int, metavar='N', help='补算最近 N 分钟')
    parser.add_argument('--interval', type=int, default=60, help='轮询间隔秒数')
    args = parser.parse_args()

    if args.once or args.backfill:
        client = get_client()
        if not client.ping():
            print("连不上 Doris")
            sys.exit(1)
        process_once(client, backfill_minutes=args.backfill)
    else:
        run_continuous(args.interval)
