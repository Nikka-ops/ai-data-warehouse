# -*- coding: utf-8 -*-
"""
AI 实时流处理器
每分钟运行一次，完成：
1. 聚合最近1分钟的流式数据 → DWS 分钟统计
2. AI 检测异常（流量突增/价格异常/品类偏移）
3. 生成告警写入 ai_quality_alerts 表
4. 触发 DWD 增量更新（支付数据关联）

运行：python kafka/stream_processor.py
或由 Airflow 每分钟调度
"""

import os
import json
import time
import uuid
from datetime import datetime, timedelta
import clickhouse_connect
import pandas as pd
from openai import OpenAI

CH_HOST     = os.getenv('CLICKHOUSE_HOST', 'localhost')
CH_PORT     = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_USER     = os.getenv('CLICKHOUSE_USER', 'admin')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )

def get_llm():
    return OpenAI(
        api_key=os.getenv('DEEPSEEK_API_KEY', ''),
        base_url='https://api.deepseek.com',
        timeout=30.0
    )


# ── 分钟级聚合 ────────────────────────────────────────────────

def aggregate_minute_window(ch, window_start: datetime, window_end: datetime) -> dict:
    """聚合指定时间窗口内的订单数据"""
    result = ch.query(f"""
        SELECT
            count()                           AS order_cnt,
            round(sum(price), 2)              AS total_gmv,
            round(avg(price), 2)              AS avg_price,
            round(max(price), 2)              AS max_price,
            countDistinct(customer_id)        AS unique_customers,
            countDistinct(product_category)   AS category_cnt,
            -- Top 品类
            argMax(product_category, order_cnt_by_cat) AS top_category,
            -- 各状态分布
            countIf(order_status = 'delivered')  AS delivered_cnt,
            countIf(order_status = 'canceled')   AS canceled_cnt
        FROM (
            SELECT *,
                count() OVER (PARTITION BY product_category) AS order_cnt_by_cat
            FROM ods.orders_stream
            WHERE event_time >= '{window_start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND event_time <  '{window_end.strftime('%Y-%m-%d %H:%M:%S')}'
        )
    """).first_row

    return {
        'order_cnt':        result[0] or 0,
        'total_gmv':        result[1] or 0.0,
        'avg_price':        result[2] or 0.0,
        'max_price':        result[3] or 0.0,
        'unique_customers': result[4] or 0,
        'category_cnt':     result[5] or 0,
        'top_category':     result[6] or '',
        'delivered_cnt':    result[7] or 0,
        'canceled_cnt':     result[8] or 0,
    }


def get_historical_baseline(ch, lookback_minutes: int = 60) -> dict:
    """获取历史基线（最近N分钟的平均水平）"""
    result = ch.query(f"""
        SELECT
            avg(order_cnt)  AS avg_order_cnt,
            avg(total_gmv)  AS avg_gmv,
            avg(avg_price)  AS avg_price,
            stddevPop(order_cnt) AS std_order_cnt
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL {lookback_minutes} MINUTE
          AND window_start < now() - INTERVAL 1 MINUTE  -- 排除最新一分钟
    """).first_row

    return {
        'avg_order_cnt': result[0] or 0,
        'avg_gmv':       result[1] or 0.0,
        'avg_price':     result[2] or 0.0,
        'std_order_cnt': result[3] or 0,
    }


def write_minute_stats(ch, window_start: datetime, window_end: datetime, stats: dict):
    """写入分钟聚合结果到 DWS"""
    ch.insert(
        'dws.realtime_minute_stats',
        [[
            window_start,
            window_end,
            stats['order_cnt'],
            stats['total_gmv'],
            stats['avg_price'],
            stats['unique_customers'],
            stats['top_category'],
            datetime.now()
        ]],
        column_names=[
            'window_start', 'window_end', 'order_cnt', 'total_gmv',
            'avg_price', 'unique_customers', 'top_category', '_created_at'
        ]
    )


# ── AI 异常检测 ───────────────────────────────────────────────

def ai_detect_anomalies(
    current: dict,
    baseline: dict,
    window_start: datetime,
    window_end: datetime
) -> list[dict]:
    """
    用 AI 分析当前窗口数据，检测异常并生成告警
    结合规则检测（快速）+ AI 分析（智能）
    """
    alerts = []

    # ── 规则检测（毫秒级，不调 LLM）────────────────────────────

    # 1. 订单量突增（超过基线3倍）
    if baseline['avg_order_cnt'] > 0:
        ratio = current['order_cnt'] / baseline['avg_order_cnt']
        if ratio > 3.0:
            alerts.append({
                'alert_type':      'ANOMALY',
                'severity':        'HIGH',
                'field_name':      'order_cnt',
                'detail':          f"订单量突增 {ratio:.1f}x，当前 {current['order_cnt']} 单，基线 {baseline['avg_order_cnt']:.0f} 单",
                'metric_value':    current['order_cnt'],
                'threshold_value': baseline['avg_order_cnt'] * 3,
                'ai_suggestion':   '',  # 稍后 AI 填充
            })
        elif current['order_cnt'] < baseline['avg_order_cnt'] * 0.2 and baseline['avg_order_cnt'] > 10:
            alerts.append({
                'alert_type':      'ANOMALY',
                'severity':        'MEDIUM',
                'field_name':      'order_cnt',
                'detail':          f"订单量骤降至基线 {current['order_cnt']/baseline['avg_order_cnt']:.0%}",
                'metric_value':    current['order_cnt'],
                'threshold_value': baseline['avg_order_cnt'] * 0.2,
                'ai_suggestion':   '',
            })

    # 2. 价格异常（极端高价）
    if current['avg_price'] > 0 and baseline['avg_price'] > 0:
        price_ratio = current['avg_price'] / baseline['avg_price']
        if price_ratio > 2.5:
            alerts.append({
                'alert_type':      'QUALITY',
                'severity':        'MEDIUM',
                'field_name':      'price',
                'detail':          f"平均价格异常偏高 {price_ratio:.1f}x：当前 R${current['avg_price']:.2f}，基线 R${baseline['avg_price']:.2f}",
                'metric_value':    current['avg_price'],
                'threshold_value': baseline['avg_price'] * 2.5,
                'ai_suggestion':   '',
            })

    # 3. 取消率异常
    if current['order_cnt'] > 0:
        cancel_rate = current['canceled_cnt'] / current['order_cnt']
        if cancel_rate > 0.15:
            alerts.append({
                'alert_type':      'QUALITY',
                'severity':        'HIGH',
                'field_name':      'order_status',
                'detail':          f"取消率异常：{cancel_rate:.1%}（阈值 15%），{current['canceled_cnt']}/{current['order_cnt']} 单取消",
                'metric_value':    cancel_rate,
                'threshold_value': 0.15,
                'ai_suggestion':   '',
            })

    # ── AI 分析（仅在有告警时调用，节省费用）──────────────────
    if alerts:
        try:
            llm = get_llm()
            alert_desc = "\n".join([f"- {a['detail']}" for a in alerts])
            prompt = f"""你是电商数据运营专家。以下是实时数据监控发现的异常：

时间窗口：{window_start.strftime('%H:%M')} ~ {window_end.strftime('%H:%M')}
当前数据：订单{current['order_cnt']}单，GMV R${current['total_gmv']:.0f}，均价 R${current['avg_price']:.2f}，取消{current['canceled_cnt']}单
历史基线：订单均值{baseline['avg_order_cnt']:.0f}单，GMV均值 R${baseline['avg_gmv']:.0f}

发现的异常：
{alert_desc}

请针对每个异常给出：1）可能原因 2）建议操作 3）紧急程度（1-3分钟内需要处理/10分钟内/1小时内）
回答简洁，每条不超过2句话，用中文。格式：
异常1：[原因]+[操作]+[紧急程度]
异常2：..."""

            resp = llm.chat.completions.create(
                model='deepseek-chat',
                messages=[{'role': 'user', 'content': prompt}],
                temperature=0.3,
                max_tokens=400,
            )
            ai_suggestion = resp.choices[0].message.content.strip()

            # 将 AI 建议分配给各告警
            suggestion_lines = ai_suggestion.split('\n')
            for i, alert in enumerate(alerts):
                if i < len(suggestion_lines):
                    alert['ai_suggestion'] = suggestion_lines[i]
                else:
                    alert['ai_suggestion'] = ai_suggestion

        except Exception as e:
            for alert in alerts:
                alert['ai_suggestion'] = f"AI分析暂不可用：{str(e)[:100]}"

    return alerts


def write_alerts(ch, alerts: list[dict], window_start: datetime, window_end: datetime):
    """将告警写入 ClickHouse"""
    if not alerts:
        return

    rows = []
    for alert in alerts:
        rows.append([
            str(uuid.uuid4()),
            datetime.now(),
            alert.get('alert_type', 'UNKNOWN'),
            alert.get('severity', 'LOW'),
            'ods.orders_stream',
            alert.get('field_name', ''),
            alert.get('detail', ''),
            alert.get('ai_suggestion', ''),
            window_start,
            window_end,
            float(alert.get('metric_value', 0)),
            float(alert.get('threshold_value', 0)),
        ])

    ch.insert(
        'stream.ai_quality_alerts',
        rows,
        column_names=[
            'alert_id', 'alert_time', 'alert_type', 'severity',
            'table_name', 'field_name', 'detail', 'ai_suggestion',
            'window_start', 'window_end', 'metric_value', 'threshold_value'
        ]
    )
    print(f"  [告警] 写入 {len(alerts)} 条告警")


# ── 增量 DWD 更新（支付数据关联）────────────────────────────

def update_dwd_with_payments(ch, window_start: datetime):
    """将最新支付数据关联更新到 DWD 宽表"""
    sql = f"""
    INSERT INTO dwd.realtime_order_detail
    SELECT
        o.order_id,
        o.customer_id,
        o.product_id,
        o.product_category,
        o.state,
        o.city,
        o.price,
        o.freight_value,
        o.price + o.freight_value  AS total_amount,
        coalesce(p.payment_type, '') AS payment_type,
        coalesce(p.payment_value, 0) AS payment_value,
        o.order_status,
        o.event_time,
        toDate(o.event_time)        AS event_date,
        toHour(o.event_time)        AS event_hour,
        if(o.order_status = 'delivered', 1, 0) AS is_paid,
        now()                       AS _ingest_time
    FROM ods.orders_stream o
    LEFT JOIN ods.payments_stream p ON o.order_id = p.order_id
    WHERE o._ingest_time >= '{(window_start - timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')}'
    """
    ch.command(sql)


# ── 主流程 ────────────────────────────────────────────────────

def process_window():
    """处理一个时间窗口（1分钟）"""
    window_end   = datetime.now().replace(second=0, microsecond=0)
    window_start = window_end - timedelta(minutes=1)

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 处理窗口 {window_start.strftime('%H:%M')} ~ {window_end.strftime('%H:%M')}")

    ch = get_ch()

    # 1. 聚合当前窗口
    print("  [1/4] 聚合分钟数据...")
    current = aggregate_minute_window(ch, window_start, window_end)
    print(f"       订单:{current['order_cnt']} GMV:R${current['total_gmv']:.0f} 均价:R${current['avg_price']:.2f}")

    if current['order_cnt'] == 0:
        print("  本窗口无数据，跳过")
        return

    # 2. 写入 DWS 分钟统计
    print("  [2/4] 写入 DWS 分钟统计...")
    write_minute_stats(ch, window_start, window_end, current)

    # 3. AI 异常检测
    print("  [3/4] AI 异常检测...")
    baseline = get_historical_baseline(ch)
    alerts   = ai_detect_anomalies(current, baseline, window_start, window_end)
    if alerts:
        print(f"  ⚠️  发现 {len(alerts)} 个异常")
        for a in alerts:
            print(f"       [{a['severity']}] {a['detail']}")
        write_alerts(ch, alerts, window_start, window_end)
    else:
        print("  ✅ 未发现异常")

    # 4. 增量更新 DWD
    print("  [4/4] 增量更新 DWD 宽表...")
    update_dwd_with_payments(ch, window_start)

    print(f"  ✅ 窗口处理完成")


def run_continuous():
    """持续运行，每分钟处理一次"""
    print("=" * 60)
    print("  AI 实时流处理器启动")
    print("  每分钟执行：聚合 → 质检 → 告警 → DWD 更新")
    print("=" * 60)

    while True:
        try:
            process_window()
        except Exception as e:
            print(f"[ERROR] 窗口处理失败：{e}")

        # 等到下一分钟整点
        now = datetime.now()
        next_minute = (now + timedelta(minutes=1)).replace(second=2, microsecond=0)
        sleep_seconds = (next_minute - now).total_seconds()
        print(f"  等待 {sleep_seconds:.0f} 秒到下一个窗口...")
        time.sleep(sleep_seconds)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--once':
        process_window()  # 只处理一次（测试用）
    else:
        run_continuous()
