# -*- coding: utf-8 -*-
"""
业务指标告警监控
每5分钟对比"当前1小时"与"昨天同时段1小时"，发现异常后调用 LLM 分析原因，
推送 Webhook，并将告警记录写入 ClickHouse stream.business_alerts。
"""

import os
import sys
import uuid
import time
import json
import urllib.request
import urllib.error
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('business_monitor')

# 告警阈值常量，集中定义便于调整
GMV_DROP_THRESHOLD         = 0.20   # GMV 下跌超过 20% 触发
CANCEL_RATE_THRESHOLD      = 0.30   # 取消率超过 30% 触发
CANCEL_RATE_DELTA          = 0.10   # 取消率比昨天同期高 10pp 触发
CATEGORY_DROP_THRESHOLD    = 0.40   # 任意品类订单量下跌超过 40% 触发


@ch_retry
def _get_ch():
    """获取 ClickHouse 连接，带自动重试"""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


# ── 数据查询 ────────────────────────────────────────────────────

def _fetch_current_metrics(ch) -> dict:
    """查询当前1小时的 GMV、订单量、取消数"""
    row = ch.query("""
        SELECT
            sum(total_gmv)  AS gmv,
            sum(order_cnt)  AS order_cnt,
            sum(cancel_cnt) AS cancel_cnt
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL 1 HOUR
          AND window_start < now()
    """).result_rows
    if not row or row[0][0] is None:
        return {'gmv': 0.0, 'order_cnt': 0, 'cancel_cnt': 0}
    r = row[0]
    return {
        'gmv':        float(r[0] or 0),
        'order_cnt':  int(r[1] or 0),
        'cancel_cnt': int(r[2] or 0),
    }


def _fetch_baseline_metrics(ch) -> dict:
    """查询昨天同时段1小时的 GMV、订单量、取消数（作为对比基准）"""
    row = ch.query("""
        SELECT
            sum(total_gmv)  AS gmv,
            sum(order_cnt)  AS order_cnt,
            sum(cancel_cnt) AS cancel_cnt
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL 25 HOUR
          AND window_start <  now() - INTERVAL 23 HOUR
    """).result_rows
    if not row or row[0][0] is None:
        return {'gmv': 0.0, 'order_cnt': 0, 'cancel_cnt': 0}
    r = row[0]
    return {
        'gmv':        float(r[0] or 0),
        'order_cnt':  int(r[1] or 0),
        'cancel_cnt': int(r[2] or 0),
    }


def _fetch_current_category_metrics(ch) -> dict:
    """查询当前1小时各品类订单量"""
    rows = ch.query("""
        SELECT product_category, sum(order_cnt) AS order_cnt
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL 1 HOUR
          AND window_start < now()
        GROUP BY product_category
    """).result_rows
    return {r[0]: int(r[1] or 0) for r in rows if r[0]}


def _fetch_baseline_category_metrics(ch) -> dict:
    """查询昨天同时段1小时各品类订单量（对比基准）"""
    rows = ch.query("""
        SELECT product_category, sum(order_cnt) AS order_cnt
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL 25 HOUR
          AND window_start <  now() - INTERVAL 23 HOUR
        GROUP BY product_category
    """).result_rows
    return {r[0]: int(r[1] or 0) for r in rows if r[0]}


# ── LLM 根因分析 ───────────────────────────────────────────────

def _analyze_root_cause(metric_name: str, current: float, baseline: float,
                         change_pct: float, detail: str) -> str:
    """调用 LLM 生成不超过100字的中文原因分析；失败时降级返回固定文本"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=20.0)
        prompt = (
            f"业务指标异常：{metric_name}\n"
            f"当前值：{current:.2f}，基准值（昨天同期）：{baseline:.2f}，"
            f"变化幅度：{change_pct:+.1f}%\n"
            f"详情：{detail}\n\n"
            "请用不超过100字的中文简要分析可能的原因（不要列编号，直接说）。"
        )
        resp = client.chat.completions.create(
            model=cfg.llm_model,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=cfg.insight_temperature,
            max_tokens=150,
        )
        return resp.choices[0].message.content.strip()[:200]
    except Exception as e:
        # LLM 不可用不应中断告警主流程
        log.warning('[LLM] 根因分析失败（跳过）：%s', e)
        return 'LLM 分析不可用'


# ── Webhook 推送 ───────────────────────────────────────────────

def _send_webhook(metric_name: str, current: float, baseline: float,
                   change_pct: float, root_cause: str) -> bool:
    """向 WEBHOOK_URL 推送告警；URL 为空则跳过，返回是否推送成功"""
    url = os.getenv('WEBHOOK_URL', '').strip()
    if not url:
        log.debug('[Webhook] WEBHOOK_URL 未配置，跳过推送')
        return False

    text = (
        f"🚨 [业务告警] {metric_name}\n"
        f"当前值：{current}\n"
        f"基准值：{baseline}\n"
        f"变化：{change_pct:.1f}%\n"
        f"原因分析：{root_cause}"
    )
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
        log.warning('[Webhook] 推送失败：%s', e)
        return False


# ── 写入 ClickHouse ────────────────────────────────────────────

def _write_alert(ch, metric_name: str, current_value: float, baseline_value: float,
                  change_pct: float, severity: str, detail: str,
                  root_cause: str, webhook_sent: bool) -> None:
    """将告警记录插入 stream.business_alerts"""
    ch.insert(
        'stream.business_alerts',
        [[
            str(uuid.uuid4()),
            datetime.now(),
            metric_name,
            current_value,
            baseline_value,
            change_pct,
            severity,
            detail,
            root_cause,
            int(webhook_sent),
            0,  # resolved=0，新告警默认未处理
        ]],
        column_names=[
            'alert_id', 'alert_time', 'metric_name',
            'current_value', 'baseline_value', 'change_pct',
            'severity', 'detail', 'root_cause',
            'webhook_sent', 'resolved',
        ],
    )
    log.info('[Alert] 已写入告警：%s  变化 %.1f%%  严重度：%s', metric_name, change_pct, severity)


# ── 各指标检查函数 ─────────────────────────────────────────────

def _check_gmv(ch, cur: dict, base: dict) -> None:
    """检查 GMV 是否下跌超过 20%"""
    if base['gmv'] <= 0:
        return  # 昨天无数据，无法对比

    change_pct = (cur['gmv'] - base['gmv']) / base['gmv'] * 100
    if change_pct >= -GMV_DROP_THRESHOLD * 100:
        return  # 未触发阈值

    severity  = 'CRITICAL' if change_pct < -35 else 'HIGH'
    detail    = (f"当前1小时 GMV={cur['gmv']:.2f}，"
                 f"昨天同期 GMV={base['gmv']:.2f}，"
                 f"下跌 {abs(change_pct):.1f}%")
    log.warning('[Monitor] GMV 告警：%s', detail)

    root_cause  = _analyze_root_cause('GMV下跌', cur['gmv'], base['gmv'], change_pct, detail)
    webhook_ok  = _send_webhook('GMV下跌', cur['gmv'], base['gmv'], change_pct, root_cause)
    _write_alert(ch, 'GMV下跌', cur['gmv'], base['gmv'], change_pct,
                 severity, detail, root_cause, webhook_ok)


def _check_cancel_rate(ch, cur: dict, base: dict) -> None:
    """检查取消率是否超过 30% 且比昨天同期高 10pp 以上"""
    if cur['order_cnt'] <= 0:
        return

    cur_rate  = cur['cancel_cnt']  / cur['order_cnt']
    base_rate = (base['cancel_cnt'] / base['order_cnt']) if base['order_cnt'] > 0 else 0.0
    delta_pp  = (cur_rate - base_rate) * 100

    # 需同时满足绝对阈值和环比阈值
    if cur_rate < CANCEL_RATE_THRESHOLD or delta_pp < CANCEL_RATE_DELTA * 100:
        return

    change_pct = delta_pp  # 用百分点差作为变化量上报
    severity   = 'CRITICAL' if cur_rate > 0.50 else 'HIGH'
    detail     = (f"当前取消率={cur_rate:.1%}，"
                  f"昨天同期取消率={base_rate:.1%}，"
                  f"高出 {delta_pp:.1f}pp")
    log.warning('[Monitor] 取消率告警：%s', detail)

    root_cause = _analyze_root_cause(
        '订单取消率异常', cur_rate * 100, base_rate * 100, change_pct, detail)
    webhook_ok = _send_webhook('订单取消率异常', cur_rate * 100, base_rate * 100,
                                change_pct, root_cause)
    _write_alert(ch, '订单取消率异常', cur_rate * 100, base_rate * 100,
                 change_pct, severity, detail, root_cause, webhook_ok)


def _check_category_drop(ch, cur_cats: dict, base_cats: dict) -> None:
    """检查是否有品类订单量下跌超过 40%，逐品类报告"""
    for cat, base_cnt in base_cats.items():
        if base_cnt <= 0:
            continue
        cur_cnt    = cur_cats.get(cat, 0)
        change_pct = (cur_cnt - base_cnt) / base_cnt * 100
        if change_pct >= -CATEGORY_DROP_THRESHOLD * 100:
            continue

        severity = 'CRITICAL' if change_pct < -60 else 'HIGH'
        detail   = (f"品类【{cat}】当前1小时订单量={cur_cnt}，"
                    f"昨天同期={base_cnt}，下跌 {abs(change_pct):.1f}%")
        log.warning('[Monitor] 品类告警：%s', detail)

        metric_name = f'品类订单量下跌:{cat}'
        root_cause  = _analyze_root_cause(
            metric_name, float(cur_cnt), float(base_cnt), change_pct, detail)
        webhook_ok  = _send_webhook(
            metric_name, float(cur_cnt), float(base_cnt), change_pct, root_cause)
        _write_alert(ch, metric_name, float(cur_cnt), float(base_cnt),
                     change_pct, severity, detail, root_cause, webhook_ok)


# ── 主流程 ────────────────────────────────────────────────────

def run_once() -> None:
    """执行一轮完整的业务指标检查"""
    log.info('[Monitor] 开始业务指标检查...')
    try:
        ch = _get_ch()
    except Exception as e:
        log.error('[Monitor] ClickHouse 连接失败，本轮跳过：%s', e)
        return

    try:
        cur  = _fetch_current_metrics(ch)
        base = _fetch_baseline_metrics(ch)
        log.debug('[Monitor] 当前=%s  基准=%s', cur, base)

        _check_gmv(ch, cur, base)
        _check_cancel_rate(ch, cur, base)

        cur_cats  = _fetch_current_category_metrics(ch)
        base_cats = _fetch_baseline_category_metrics(ch)
        _check_category_drop(ch, cur_cats, base_cats)

        log.info('[Monitor] 本轮检查完成')
    except Exception as e:
        log.error('[Monitor] 检查过程出错：%s', e)


def run_loop(interval: int = 300):
    """每5分钟检查一次，持续运行"""
    log.info('[Monitor] 业务指标监控启动，间隔 %ds', interval)
    while True:
        run_once()
        time.sleep(interval)


if __name__ == '__main__':
    run_loop()
