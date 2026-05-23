# -*- coding: utf-8 -*-
"""
定时报告推送
每10分钟检查是否到达发报时间：
  - 日报：每天 09:00
  - 周报：每周一 09:00
用文件锁（/tmp/report_*.lock）防止同一时间窗口重复发送。
"""

import os
import sys
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('report_scheduler')


@ch_retry
def _get_ch():
    """获取 ClickHouse 连接，带自动重试"""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


# ── 文件锁工具 ────────────────────────────────────────────────────

def _lock_path_daily() -> str:
    """今日日报锁文件路径，按日期唯一"""
    return f"/tmp/report_daily_{date.today().strftime('%Y%m%d')}.lock"


def _lock_path_weekly() -> str:
    """本周周报锁文件路径，按 ISO 年-周唯一"""
    y, w, _ = date.today().isocalendar()
    return f"/tmp/report_weekly_{y}{w:02d}.lock"


def _is_locked(lock_file: str) -> bool:
    """锁文件存在即视为已发送，防止重复推送"""
    return os.path.exists(lock_file)


def _acquire_lock(lock_file: str) -> None:
    """创建锁文件，标记本时间窗口已推送"""
    try:
        with open(lock_file, 'w') as f:
            f.write(datetime.now().isoformat())
    except Exception as e:
        log.warning('[Lock] 创建锁文件失败 %s：%s', lock_file, e)


# ── Webhook 推送 ───────────────────────────────────────────────

def _send_webhook(text: str) -> bool:
    """向 WEBHOOK_URL 推送文本；URL 为空则跳过"""
    url = os.getenv('WEBHOOK_URL', '').strip()
    if not url:
        log.debug('[Webhook] WEBHOOK_URL 未配置，跳过推送')
        return False

    payload = json.dumps({'text': text}).encode('utf-8')
    req = urllib.request.Request(
        url, data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            log.info('[Webhook] 推送成功，状态码：%s', resp.status)
            return True
    except urllib.error.URLError as e:
        # 推送失败只警告，不中断报告生成流程
        log.warning('[Webhook] 推送失败：%s', e)
        return False


# ── LLM 运营建议 ───────────────────────────────────────────────

def _generate_advice(context: str) -> str:
    """调用 LLM 生成不超过50字的运营建议；失败时返回空字符串"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=20.0)
        prompt = (
            f"以下是今日电商运营数据摘要：\n{context}\n\n"
            "请用不超过50字给出一条精简运营建议（直接给建议，不要编号或前缀）。"
        )
        resp = client.chat.completions.create(
            model=cfg.llm_model,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=cfg.insight_temperature,
            max_tokens=100,
        )
        return resp.choices[0].message.content.strip()[:100]
    except Exception as e:
        # LLM 不可用不影响报告发出
        log.warning('[LLM] 运营建议生成失败：%s', e)
        return ''


# ── 日报数据查询 ───────────────────────────────────────────────

def _fetch_daily_data(ch) -> dict:
    """查询昨日与前天数据，用于生成日报"""
    data = {}

    # 昨日聚合：GMV、订单量、取消数
    try:
        row = ch.query("""
            SELECT
                round(sum(total_gmv), 2)  AS gmv,
                sum(order_cnt)             AS order_cnt,
                sum(cancel_cnt)            AS cancel_cnt
            FROM dws.realtime_minute_stats
            WHERE window_start >= today() - INTERVAL 1 DAY
              AND window_start <  today()
        """).result_rows[0]
        data['yesterday'] = {
            'gmv':        float(row[0] or 0),
            'order_cnt':  int(row[1] or 0),
            'cancel_cnt': int(row[2] or 0),
        }
    except Exception as e:
        log.warning('[DailyReport] 昨日数据查询失败：%s', e)
        data['yesterday'] = {'gmv': 0.0, 'order_cnt': 0, 'cancel_cnt': 0}

    # 前天聚合：用于计算环比
    try:
        row = ch.query("""
            SELECT
                round(sum(total_gmv), 2)  AS gmv,
                sum(order_cnt)             AS order_cnt,
                sum(cancel_cnt)            AS cancel_cnt
            FROM dws.realtime_minute_stats
            WHERE window_start >= today() - INTERVAL 2 DAY
              AND window_start <  today() - INTERVAL 1 DAY
        """).result_rows[0]
        data['day_before'] = {
            'gmv':        float(row[0] or 0),
            'order_cnt':  int(row[1] or 0),
            'cancel_cnt': int(row[2] or 0),
        }
    except Exception as e:
        log.warning('[DailyReport] 前天数据查询失败：%s', e)
        data['day_before'] = {'gmv': 0.0, 'order_cnt': 0, 'cancel_cnt': 0}

    # 昨日 Top3 品类（按 GMV 降序）
    try:
        rows = ch.query("""
            SELECT product_category,
                   round(sum(total_gmv), 2) AS gmv,
                   sum(order_cnt)            AS order_cnt
            FROM dws.realtime_minute_stats
            WHERE window_start >= today() - INTERVAL 1 DAY
              AND window_start <  today()
              AND product_category != ''
            GROUP BY product_category
            ORDER BY gmv DESC
            LIMIT 3
        """).result_rows
        data['top3_categories'] = [
            {'cat': r[0], 'gmv': float(r[1] or 0), 'order_cnt': int(r[2] or 0)}
            for r in rows
        ]
    except Exception as e:
        log.warning('[DailyReport] Top3 品类查询失败：%s', e)
        data['top3_categories'] = []

    # 未处理告警数（表不存在时跳过，不影响日报生成）
    data['pending_alerts'] = 0
    try:
        row = ch.query("""
            SELECT count() FROM stream.business_alerts WHERE resolved = 0
        """).result_rows[0]
        data['pending_alerts'] = int(row[0] or 0)
    except Exception as e:
        log.debug('[DailyReport] 告警表查询失败（可能表不存在）：%s', e)

    return data


def _build_daily_text(data: dict, advice: str) -> str:
    """将昨日数据 dict 拼装为可读报告文本"""
    y   = data['yesterday']
    db  = data['day_before']
    top = data['top3_categories']

    # 计算环比，避免除零
    def _pct(cur, prev):
        if prev <= 0:
            return 'N/A'
        v = (cur - prev) / prev * 100
        sign = '+' if v >= 0 else ''
        return f'{sign}{v:.1f}%'

    cancel_rate = (y['cancel_cnt'] / y['order_cnt'] * 100) if y['order_cnt'] > 0 else 0.0

    lines = [
        f"昨日总 GMV：{y['gmv']:,.2f}（环比 {_pct(y['gmv'], db['gmv'])}）",
        f"昨日总订单：{y['order_cnt']:,}（环比 {_pct(y['order_cnt'], db['order_cnt'])}）",
        f"昨日取消率：{cancel_rate:.1f}%",
        "",
        "Top3 品类（按 GMV）：",
    ]
    if top:
        for i, c in enumerate(top, 1):
            lines.append(f"  {i}. {c['cat']}  GMV={c['gmv']:,.2f}  订单={c['order_cnt']:,}")
    else:
        lines.append("  （暂无品类数据）")

    lines += [
        "",
        f"当前待处理告警：{data['pending_alerts']} 条",
    ]
    if advice:
        lines += ["", f"运营建议：{advice}"]

    return '\n'.join(lines)


# ── 周报数据查询 ───────────────────────────────────────────────

def _fetch_weekly_data(ch) -> dict:
    """查询本周与上周汇总数据，用于生成周报"""
    data = {}

    # 本周：从上周一00:00到今天
    today = date.today()
    # ISO 周一为第一天
    week_start = today - timedelta(days=today.weekday())
    last_week_start = week_start - timedelta(weeks=1)

    def _week_query(start: date, end: date) -> dict:
        row = ch.query(f"""
            SELECT
                round(sum(total_gmv), 2)                         AS gmv,
                sum(order_cnt)                                    AS order_cnt,
                if(sum(order_cnt) > 0,
                   round(sum(total_gmv) / sum(order_cnt), 2), 0) AS avg_order_value,
                sum(cancel_cnt)                                   AS cancel_cnt
            FROM dws.realtime_minute_stats
            WHERE window_start >= toDate('{start}')
              AND window_start <  toDate('{end}')
        """).result_rows[0]
        return {
            'gmv':             float(row[0] or 0),
            'order_cnt':       int(row[1] or 0),
            'avg_order_value': float(row[2] or 0),
            'cancel_cnt':      int(row[3] or 0),
        }

    try:
        data['this_week'] = _week_query(week_start, today + timedelta(days=1))
    except Exception as e:
        log.warning('[WeeklyReport] 本周数据查询失败：%s', e)
        data['this_week'] = {'gmv': 0.0, 'order_cnt': 0, 'avg_order_value': 0.0, 'cancel_cnt': 0}

    try:
        data['last_week'] = _week_query(last_week_start, week_start)
    except Exception as e:
        log.warning('[WeeklyReport] 上周数据查询失败：%s', e)
        data['last_week'] = {'gmv': 0.0, 'order_cnt': 0, 'avg_order_value': 0.0, 'cancel_cnt': 0}

    # 本周告警次数（表不存在时跳过）
    data['alert_count'] = 0
    try:
        row = ch.query(f"""
            SELECT count()
            FROM stream.business_alerts
            WHERE alert_time >= toDate('{week_start}')
        """).result_rows[0]
        data['alert_count'] = int(row[0] or 0)
    except Exception as e:
        log.debug('[WeeklyReport] 告警表查询失败（可能表不存在）：%s', e)

    return data


def _build_weekly_text(data: dict) -> str:
    """将本周数据 dict 拼装为可读周报文本"""
    tw = data['this_week']
    lw = data['last_week']

    def _pct(cur, prev):
        if prev <= 0:
            return 'N/A'
        v = (cur - prev) / prev * 100
        sign = '+' if v >= 0 else ''
        return f'{sign}{v:.1f}%'

    lines = [
        f"本周 GMV：{tw['gmv']:,.2f}（vs 上周 {_pct(tw['gmv'], lw['gmv'])}）",
        f"本周订单量：{tw['order_cnt']:,}（vs 上周 {_pct(tw['order_cnt'], lw['order_cnt'])}）",
        f"本周平均客单价：{tw['avg_order_value']:,.2f}"
        f"（上周：{lw['avg_order_value']:,.2f}，"
        f"vs 上周 {_pct(tw['avg_order_value'], lw['avg_order_value'])}）",
        "",
        f"本周出现告警次数：{data['alert_count']} 次",
    ]
    return '\n'.join(lines)


# ── 报告触发逻辑 ──────────────────────────────────────────────

def _should_send_daily() -> bool:
    """当前小时等于9点且当天锁未创建时触发日报"""
    return datetime.now().hour == 9 and not _is_locked(_lock_path_daily())


def _should_send_weekly() -> bool:
    """当前是周一9点且本周锁未创建时触发周报"""
    now = datetime.now()
    return now.weekday() == 0 and now.hour == 9 and not _is_locked(_lock_path_weekly())


def send_daily_report() -> None:
    """生成并推送日报，推送完成后创建锁文件防止重复"""
    log.info('[DailyReport] 开始生成日报...')
    try:
        ch = _get_ch()
    except Exception as e:
        log.error('[DailyReport] ClickHouse 连接失败：%s', e)
        return

    try:
        data   = _fetch_daily_data(ch)
        # 将关键指标摘要传给 LLM 生成建议
        y = data['yesterday']
        cancel_rate = (y['cancel_cnt'] / y['order_cnt'] * 100) if y['order_cnt'] > 0 else 0.0
        ctx = (
            f"昨日GMV={y['gmv']:.2f}，订单量={y['order_cnt']}，"
            f"取消率={cancel_rate:.1f}%，待处理告警={data['pending_alerts']}条"
        )
        advice = _generate_advice(ctx)
        body   = _build_daily_text(data, advice)
        text   = f"📊 每日运营报告\n{body}"

        log.info('[DailyReport] 报告内容生成完毕，准备推送')
        _send_webhook(text)
        _acquire_lock(_lock_path_daily())
        log.info('[DailyReport] 日报发送完成')
    except Exception as e:
        log.error('[DailyReport] 生成日报时出错：%s', e)


def send_weekly_report() -> None:
    """生成并推送周报，推送完成后创建锁文件防止重复"""
    log.info('[WeeklyReport] 开始生成周报...')
    try:
        ch = _get_ch()
    except Exception as e:
        log.error('[WeeklyReport] ClickHouse 连接失败：%s', e)
        return

    try:
        data = _fetch_weekly_data(ch)
        body = _build_weekly_text(data)
        text = f"📈 每周运营报告\n{body}"

        log.info('[WeeklyReport] 报告内容生成完毕，准备推送')
        _send_webhook(text)
        _acquire_lock(_lock_path_weekly())
        log.info('[WeeklyReport] 周报发送完成')
    except Exception as e:
        log.error('[WeeklyReport] 生成周报时出错：%s', e)


# ── 主循环 ────────────────────────────────────────────────────

def run_loop(check_interval: int = 600):
    """每10分钟检查一次是否到了发报告时间，防止重复发送用文件锁"""
    log.info('[Scheduler] 定时报告调度器启动，检查间隔 %ds', check_interval)
    while True:
        try:
            if _should_send_daily():
                send_daily_report()
            if _should_send_weekly():
                send_weekly_report()
        except Exception as e:
            # 顶层捕获，保证主循环不因意外退出
            log.error('[Scheduler] 调度异常：%s', e)
        time.sleep(check_interval)


if __name__ == '__main__':
    run_loop()
