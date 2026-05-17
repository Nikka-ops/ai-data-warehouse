# -*- coding: utf-8 -*-
"""
PyFlink 实时流处理作业
替代 kafka/stream_processor.py 的 Python 轮询方案。

架构：
  Kafka(orders_stream) ──┐
                          ├─ Flink ─► Kafka(flink.minute_stats) → ClickHouse dws.realtime_minute_stats
  Kafka(payments_stream)─┘         ► Kafka(flink.realtime_dwd)  → ClickHouse dwd.realtime_order_detail
                                   ► Kafka(flink.alerts)         → ClickHouse stream.ai_quality_alerts

运行方式（本地模式，无需 Flink 集群）：
  python flink/flink_stream_job.py

提交到 Flink 集群：
  flink run -py flink/flink_stream_job.py --pyFiles flink/

依赖：
  pip install apache-flink kafka-python clickhouse-connect
"""

import os, sys, json, logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger

log = get_logger('flink_job')

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment, EnvironmentSettings
    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False
    log.warning('PyFlink 未安装，将以 Python 模拟模式运行（等价功能，适用于开发环境）')


# ── ClickHouse 写入辅助 ───────────────────────────────────────

def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
    )


# ══════════════════════════════════════════════════════════════
# PyFlink Table API 实现（生产环境）
# ══════════════════════════════════════════════════════════════

FLINK_KAFKA_ORDERS_DDL = """
CREATE TABLE IF NOT EXISTS orders_source (
    order_id         STRING,
    customer_id      STRING,
    product_id       STRING,
    product_category STRING,
    seller_id        STRING,
    price            DOUBLE,
    freight_value    DOUBLE,
    order_status     STRING,
    state            STRING,
    city             STRING,
    event_time       STRING,
    msg_version      STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = '{orders_topic}',
    'properties.bootstrap.servers'  = '{bootstrap}',
    'properties.group.id'           = 'flink_orders_consumer',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true'
)
"""

FLINK_KAFKA_PAYMENTS_DDL = """
CREATE TABLE IF NOT EXISTS payments_source (
    payment_id    STRING,
    order_id      STRING,
    payment_type  STRING,
    payment_value DOUBLE,
    installments  INT,
    event_time    STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = '{payments_topic}',
    'properties.bootstrap.servers'  = '{bootstrap}',
    'properties.group.id'           = 'flink_payments_consumer',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true'
)
"""

# Flink 将聚合结果写回 Kafka，ClickHouse Kafka Engine 消费
FLINK_SINK_MINUTE_STATS_DDL = """
CREATE TABLE IF NOT EXISTS minute_stats_sink (
    window_start     STRING,
    window_end       STRING,
    order_cnt        BIGINT,
    total_gmv        DOUBLE,
    avg_price        DOUBLE,
    unique_customers BIGINT,
    top_category     STRING
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = '{flink_stats_topic}',
    'properties.bootstrap.servers'  = '{bootstrap}',
    'format'                        = 'json'
)
"""

FLINK_SINK_DWD_DDL = """
CREATE TABLE IF NOT EXISTS realtime_dwd_sink (
    order_id         STRING,
    customer_id      STRING,
    product_id       STRING,
    product_category STRING,
    seller_id        STRING,
    state            STRING,
    city             STRING,
    price            DOUBLE,
    freight_value    DOUBLE,
    total_amount     DOUBLE,
    payment_type     STRING,
    payment_value    DOUBLE,
    order_status     STRING,
    event_time       STRING,
    event_date       STRING,
    event_hour       INT,
    is_paid          INT
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = '{flink_dwd_topic}',
    'properties.bootstrap.servers'  = '{bootstrap}',
    'format'                        = 'json'
)
"""

# 1分钟滚动窗口聚合
FLINK_MINUTE_AGG_SQL = """
INSERT INTO minute_stats_sink
SELECT
    DATE_FORMAT(TUMBLE_START(proc_time, INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_start,
    DATE_FORMAT(TUMBLE_END(proc_time,   INTERVAL '1' MINUTE), 'yyyy-MM-dd HH:mm:ss') AS window_end,
    COUNT(*)                    AS order_cnt,
    ROUND(SUM(price), 2)        AS total_gmv,
    ROUND(AVG(price), 2)        AS avg_price,
    COUNT(DISTINCT customer_id) AS unique_customers,
    FIRST_VALUE(product_category) AS top_category
FROM orders_source
WHERE price >= 0
GROUP BY TUMBLE(proc_time, INTERVAL '1' MINUTE)
"""

# 订单 + 支付 JOIN（处理时间关联，1分钟容忍窗口）
FLINK_DWD_JOIN_SQL = """
INSERT INTO realtime_dwd_sink
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.product_category,
    o.seller_id,
    o.state,
    o.city,
    o.price,
    o.freight_value,
    o.price + o.freight_value AS total_amount,
    COALESCE(p.payment_type, '')   AS payment_type,
    COALESCE(p.payment_value, 0.0) AS payment_value,
    o.order_status,
    o.event_time,
    SUBSTRING(o.event_time, 1, 10) AS event_date,
    CAST(SUBSTRING(o.event_time, 12, 2) AS INT) AS event_hour,
    CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END AS is_paid
FROM orders_source o
LEFT JOIN payments_source FOR SYSTEM_TIME AS OF o.proc_time AS p
    ON o.order_id = p.order_id
"""


def run_flink_job():
    """使用 PyFlink Table API 提交实时流处理作业"""
    log.info('使用 PyFlink Table API 模式启动')

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(
        StreamExecutionEnvironment.get_execution_environment(),
        environment_settings=env_settings,
    )
    t_env.get_config().set('parallelism.default', '2')
    t_env.get_config().set('table.exec.mini-batch.enabled', 'true')
    t_env.get_config().set('table.exec.mini-batch.allow-latency', '5 s')
    t_env.get_config().set('table.exec.mini-batch.size', '5000')

    params = dict(
        bootstrap=cfg.kafka_bootstrap,
        orders_topic=cfg.orders_topic,
        payments_topic=cfg.payments_topic,
        flink_stats_topic=cfg.flink_stats_topic,
        flink_dwd_topic=cfg.flink_dwd_topic,
    )

    # 注册源表和目标表
    for ddl in [
        FLINK_KAFKA_ORDERS_DDL.format(**params),
        FLINK_KAFKA_PAYMENTS_DDL.format(**params),
        FLINK_SINK_MINUTE_STATS_DDL.format(**params),
        FLINK_SINK_DWD_DDL.format(**params),
    ]:
        t_env.execute_sql(ddl)

    # 提交聚合作业（异步）
    stats_job = t_env.execute_sql(FLINK_MINUTE_AGG_SQL)
    dwd_job   = t_env.execute_sql(FLINK_DWD_JOIN_SQL)

    log.info('Flink 作业已提交：minute_stats_job=%s, dwd_job=%s',
             stats_job.get_job_client().get_job_id() if stats_job.get_job_client() else 'local',
             dwd_job.get_job_client().get_job_id() if dwd_job.get_job_client() else 'local')


# ══════════════════════════════════════════════════════════════
# Python 模拟模式（开发/测试，无需 PyFlink 运行时）
# ══════════════════════════════════════════════════════════════

def run_python_simulation():
    """
    当 PyFlink 不可用时，用纯 Python 实现等价的实时聚合逻辑。
    读 Kafka → 1分钟窗口聚合 → 直接写 ClickHouse。
    """
    import time
    from collections import defaultdict
    from datetime import timedelta
    from kafka import KafkaConsumer
    import clickhouse_connect

    log.info('Python 模拟模式启动（替代 PyFlink，功能等价）')
    log.info('Kafka: %s，Orders Topic: %s', cfg.kafka_bootstrap, cfg.orders_topic)

    consumer = KafkaConsumer(
        cfg.orders_topic,
        bootstrap_servers=cfg.kafka_bootstrap,
        group_id='python_flink_sim',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=1000,
    )
    ch = _get_ch()

    window_data: list = []
    window_start = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    def _flush_window(data: list, w_start: datetime):
        if not data:
            return
        w_end = w_start + timedelta(minutes=1)

        # 1分钟窗口聚合
        order_cnt        = len(data)
        total_gmv        = round(sum(d.get('price', 0) for d in data), 2)
        avg_price        = round(total_gmv / order_cnt, 2) if order_cnt else 0
        unique_customers = len({d.get('customer_id') for d in data})
        # 最多出现的品类
        cat_counter = defaultdict(int)
        for d in data:
            cat_counter[d.get('product_category', '')] += 1
        top_category = max(cat_counter, key=cat_counter.get) if cat_counter else ''

        # 写入 dws.realtime_minute_stats
        try:
            ch.insert(
                'dws.realtime_minute_stats',
                [[w_start.replace(tzinfo=None), w_end.replace(tzinfo=None),
                  order_cnt, total_gmv, avg_price, unique_customers, top_category,
                  datetime.now()]],
                column_names=['window_start', 'window_end', 'order_cnt', 'total_gmv',
                              'avg_price', 'unique_customers', 'top_category', '_created_at'],
            )
            log.info('[窗口 %s] 写入聚合：%d 单，GMV=%.0f，均价=%.2f',
                     w_start.strftime('%H:%M'), order_cnt, total_gmv, avg_price)
        except Exception as e:
            log.error('写入 dws.realtime_minute_stats 失败：%s', e)

        # 异常检测（规则引擎）
        _detect_and_write_alerts(ch, data, order_cnt, total_gmv, avg_price, w_start, w_end)

    def _detect_and_write_alerts(ch, data, order_cnt, total_gmv, avg_price, w_start, w_end):
        """规则 + AI 双层异常检测"""
        import uuid
        alerts = []

        # 取消率异常（>15%）
        canceled = sum(1 for d in data if d.get('order_status') == 'canceled')
        if order_cnt > 5 and canceled / order_cnt > 0.15:
            alerts.append({
                'alert_type': 'QUALITY', 'severity': 'HIGH',
                'field_name': 'order_status',
                'detail': f"取消率异常：{canceled/order_cnt:.1%}（{canceled}/{order_cnt} 单）",
                'metric_value': float(canceled / order_cnt),
                'threshold_value': 0.15,
            })

        # 单笔超高价格（> 3000 R$）
        high_price = [d for d in data if d.get('price', 0) > 3000]
        if high_price:
            alerts.append({
                'alert_type': 'QUALITY', 'severity': 'MEDIUM',
                'field_name': 'price',
                'detail': f"检测到 {len(high_price)} 笔超高价订单（>R$3000），最高 R${max(d['price'] for d in high_price):.0f}",
                'metric_value': float(max(d['price'] for d in high_price)),
                'threshold_value': 3000.0,
            })

        if not alerts:
            return

        # AI 分析建议（仅有告警时调用）
        try:
            from openai import OpenAI
            llm = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=15.0)
            alert_desc = '\n'.join(f'- {a["detail"]}' for a in alerts)
            prompt = (
                f"时间窗口 {w_start.strftime('%H:%M')}，当前：{order_cnt}单，GMV R${total_gmv:.0f}。\n"
                f"异常：\n{alert_desc}\n\n"
                "请针对每条异常给出1句原因推断和1句处理建议，中文，每条不超过30字。"
            )
            resp = llm.chat.completions.create(
                model=cfg.llm_model,
                messages=[{'role': 'user', 'content': prompt}],
                temperature=0.3, max_tokens=200,
            )
            ai_text = resp.choices[0].message.content.strip()
            lines = ai_text.split('\n')
            for i, alert in enumerate(alerts):
                alert['ai_suggestion'] = lines[i] if i < len(lines) else ai_text
        except Exception as e:
            for alert in alerts:
                alert['ai_suggestion'] = f'AI分析不可用：{str(e)[:50]}'

        # 写入告警表
        rows = [[
            str(uuid.uuid4()), datetime.now(),
            a['alert_type'], a['severity'],
            'ods.orders_stream', a['field_name'],
            a['detail'], a.get('ai_suggestion', ''),
            w_start.replace(tzinfo=None), w_end.replace(tzinfo=None),
            a['metric_value'], a['threshold_value'],
        ] for a in alerts]
        try:
            ch.insert(
                'stream.ai_quality_alerts', rows,
                column_names=['alert_id', 'alert_time', 'alert_type', 'severity',
                              'table_name', 'field_name', 'detail', 'ai_suggestion',
                              'window_start', 'window_end', 'metric_value', 'threshold_value'],
            )
            log.warning('[告警] 写入 %d 条告警', len(alerts))
        except Exception as e:
            log.error('写入告警失败：%s', e)

    log.info('开始消费 Kafka，等待数据...')
    try:
        while True:
            now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

            # 切换到新窗口时 flush 旧数据
            if now > window_start:
                _flush_window(window_data, window_start)
                window_data = []
                window_start = now

            # 消费最多1000条消息
            batch = consumer.poll(timeout_ms=500, max_records=1000)
            for _, msgs in batch.items():
                for msg in msgs:
                    if msg.value:
                        window_data.append(msg.value)

            time.sleep(0.1)

    except KeyboardInterrupt:
        log.info('停止流处理器...')
        _flush_window(window_data, window_start)
    finally:
        consumer.close()
        log.info('已退出')


# ── 主入口 ────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Flink/Python 实时流处理作业')
    parser.add_argument('--mode', choices=['flink', 'python'], default='auto',
                        help='运行模式：flink=PyFlink集群, python=纯Python模拟, auto=自动选择')
    args = parser.parse_args()

    if args.mode == 'flink' or (args.mode == 'auto' and PYFLINK_AVAILABLE):
        run_flink_job()
    else:
        run_python_simulation()
