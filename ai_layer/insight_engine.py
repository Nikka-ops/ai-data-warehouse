# -*- coding: utf-8 -*-
"""
主动洞察引擎
每5分钟自动巡检多维数据，发现有价值的趋势/异常/机会，
用 LLM 生成自然语言数据故事，写入 stream.proactive_insights。
"""

import os, sys, json, uuid, time, argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry, llm_retry

log = get_logger('insight_engine')

# 连续两轮数据变化超过此阈值才认为值得报告
TREND_THRESHOLD   = 0.15   # 15% 变化触发趋势洞察
ANOMALY_THRESHOLD = 0.40   # 40% 偏差触发异常洞察


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=30,
    )


# ── 数据采集 ──────────────────────────────────────────────────

def _collect_data(ch) -> dict:
    """采集当前时段多维度数据快照"""
    data = {}

    # 最近30分钟分钟统计（趋势判断）
    rows = ch.query("""
        SELECT window_start, order_cnt, total_gmv, avg_price, top_category
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL 30 MINUTE
        ORDER BY window_start
    """).result_rows
    data['minute_stats'] = [
        {'t': str(r[0]), 'order_cnt': r[1], 'gmv': float(r[2]),
         'avg_price': float(r[3]), 'top_cat': r[4]}
        for r in rows
    ]

    # 今日 vs 昨日同时段对比（如有数据）
    try:
        today_row = ch.query("""
            SELECT count() AS order_cnt, round(sum(price),2) AS gmv,
                   round(avg(price),2) AS avg_price
            FROM ods.orders_stream
            WHERE event_time >= today()
        """).result_rows[0]
        data['today'] = {'order_cnt': today_row[0], 'gmv': float(today_row[1]),
                         'avg_price': float(today_row[2])}
    except Exception:
        data['today'] = {}

    # 今日品类 Top5
    try:
        cat_rows = ch.query("""
            SELECT product_category, order_cnt, round(gmv,2) AS gmv
            FROM ads.realtime_category_today
            LIMIT 5
        """).result_rows
        data['top_categories'] = [
            {'cat': r[0], 'order_cnt': r[1], 'gmv': float(r[2])} for r in cat_rows
        ]
    except Exception:
        data['top_categories'] = []

    # 今日 Top3 州
    try:
        state_rows = ch.query("""
            SELECT state, order_cnt, round(gmv,2) AS gmv
            FROM ads.realtime_state_today
            ORDER BY gmv DESC LIMIT 3
        """).result_rows
        data['top_states'] = [
            {'state': r[0], 'order_cnt': r[1], 'gmv': float(r[2])} for r in state_rows
        ]
    except Exception:
        data['top_states'] = []

    # 最新告警
    try:
        alert_rows = ch.query("""
            SELECT severity, detail
            FROM stream.ai_quality_alerts
            WHERE alert_time >= now() - INTERVAL 10 MINUTE
            ORDER BY alert_time DESC LIMIT 3
        """).result_rows
        data['recent_alerts'] = [{'severity': r[0], 'detail': r[1]} for r in alert_rows]
    except Exception:
        data['recent_alerts'] = []

    return data


# ── 变化量计算 ─────────────────────────────────────────────────

def _analyze_trends(data: dict) -> list[dict]:
    """从数据快照中提取有意义的趋势信号"""
    signals = []
    stats = data.get('minute_stats', [])
    if len(stats) < 6:
        return signals

    # 对比前半段和后半段的均值
    mid = len(stats) // 2
    first_half = stats[:mid]
    second_half = stats[mid:]

    for metric in ('order_cnt', 'gmv'):
        avg_first  = sum(r[metric] for r in first_half)  / len(first_half)
        avg_second = sum(r[metric] for r in second_half) / len(second_half)
        if avg_first <= 0:
            continue
        change = (avg_second - avg_first) / avg_first

        if abs(change) >= TREND_THRESHOLD:
            signals.append({
                'type':   'trend_up' if change > 0 else 'trend_down',
                'metric': metric,
                'change': round(change * 100, 1),
                'recent_avg': round(avg_second, 1),
                'prior_avg':  round(avg_first, 1),
            })

    # 取消率信号
    if data.get('today'):
        cancel_rows = []
        try:
            ch = _get_ch()
            r = ch.query("""
                SELECT countIf(order_status='canceled') AS c, count() AS t
                FROM ods.orders_stream WHERE event_time >= now() - INTERVAL 10 MINUTE
            """).result_rows[0]
            if r[1] > 10:
                rate = r[0] / r[1]
                if rate > 0.10:
                    signals.append({
                        'type': 'anomaly', 'metric': 'cancel_rate',
                        'change': round(rate * 100, 1),
                        'detail': f'近10分钟取消率 {rate:.1%}'
                    })
        except Exception:
            pass

    return signals


# ── LLM 生成洞察文本 ───────────────────────────────────────────

_INSIGHT_PROMPT = """你是一位资深电商数据分析师，根据以下实时数据快照生成一篇简洁有力的数据洞察报告。

【当前数据快照】
{data_summary}

【检测到的趋势信号】
{signals}

【输出要求】
1. 标题：一句话概括最重要的发现（≤30字，不加标点之外的任何前缀）
2. 正文：3~5句话，必须引用具体数字，说明趋势方向、可能原因、建议关注点
3. 洞察类型（只选一个）：trend_up / trend_down / anomaly / summary / opportunity
4. 输出格式严格为 JSON：
{{
  "insight_type": "...",
  "title": "...",
  "content": "..."
}}
只返回 JSON，不要任何额外内容。"""


@llm_retry
def _generate_insight_text(data: dict, signals: list[dict]) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=30.0)

    # 构建精简数据摘要
    stats = data.get('minute_stats', [])
    last5 = stats[-5:] if len(stats) >= 5 else stats
    summary = {
        'last_5min_order_cnt': [s['order_cnt'] for s in last5],
        'last_5min_gmv':       [round(s['gmv'], 0) for s in last5],
        'today':               data.get('today', {}),
        'top_categories':      data.get('top_categories', [])[:3],
        'top_states':          data.get('top_states', [])[:3],
        'recent_alerts_count': len(data.get('recent_alerts', [])),
    }

    resp = client.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': _INSIGHT_PROMPT.format(
            data_summary=json.dumps(summary, ensure_ascii=False, indent=2),
            signals=json.dumps(signals, ensure_ascii=False, indent=2) if signals else '[]（无显著趋势）',
        )}],
        temperature=0.6,
        max_tokens=500,
    )
    raw = resp.choices[0].message.content.strip()

    import re
    match = re.search(r'\{.*\}', raw, re.DOTALL)
    if not match:
        return {'insight_type': 'summary', 'title': '实时数据快照', 'content': raw[:300]}

    result = json.loads(match.group())
    return {
        'insight_type': result.get('insight_type', 'summary'),
        'title':        result.get('title', '数据洞察')[:40],
        'content':      result.get('content', ''),
    }


# ── 主流程 ────────────────────────────────────────────────────

def run_once() -> dict:
    ch = _get_ch()
    now = datetime.now()
    period_start = now - timedelta(minutes=30)

    log.info('主动洞察引擎运行中...')
    data    = _collect_data(ch)
    signals = _analyze_trends(data)

    # 无信号且无告警时，每5轮才生成一次 summary（节省 LLM 调用）
    if not signals and not data.get('recent_alerts'):
        try:
            last_insight_time = ch.query("""
                SELECT max(generated_at) FROM stream.proactive_insights
            """).result_rows[0][0]
            if last_insight_time and (now - last_insight_time).total_seconds() < 300:
                log.info('无新信号且近5分钟已有洞察，跳过本轮')
                return {'skipped': True}
        except Exception:
            pass

    # 生成洞察
    result = _generate_insight_text(data, signals)

    # 写入 ClickHouse
    ch.insert(
        'stream.proactive_insights',
        [[
            str(uuid.uuid4()), now, period_start, now,
            result['insight_type'], result['title'], result['content'],
            json.dumps({'signals': signals, 'alerts': len(data.get('recent_alerts', []))},
                       ensure_ascii=False),
            30 if signals else 60,
            now,
        ]],
        column_names=['insight_id', 'generated_at', 'period_start', 'period_end',
                      'insight_type', 'title', 'content', 'data_context',
                      'priority', '_created_at'],
    )
    log.info('[洞察] [%s] %s', result['insight_type'], result['title'])
    return result


def run_loop(interval: int = 300):
    log.info('主动洞察引擎启动，每 %ds 一次', interval)
    while True:
        try:
            run_once()
        except Exception as e:
            log.error('洞察生成失败：%s', e)
        time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='主动洞察引擎')
    parser.add_argument('--loop', type=int, default=300, help='循环间隔秒数（0=单次）')
    args = parser.parse_args()
    if args.loop > 0:
        run_loop(args.loop)
    else:
        print(json.dumps(run_once(), ensure_ascii=False, indent=2))
