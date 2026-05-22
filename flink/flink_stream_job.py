# -*- coding: utf-8 -*-
"""
Kappa 架构：Flink 统一流处理作业
- 实时模式：消费 Kafka 最新数据，1分钟窗口聚合 → ClickHouse
- 回放模式：从 Kafka offset=earliest（或指定时间）重新消费，重算历史聚合
  用于数据修复、逻辑变更后重算，结果幂等写入 dws.kappa_hourly_agg

架构：
  Kafka(orders_stream)  ──┐
                           ├─ Flink ─► dws.realtime_minute_stats（实时1分钟聚合）
  Kafka(payments_stream)─┘          ► dwd.realtime_order_detail（DWD宽表）
                                    ► stream.ai_quality_alerts（AI质检告警）
                                    ► dws.kappa_hourly_agg（回放模式小时聚合）
"""

import os, sys, json, time, logging, uuid
from datetime import datetime, timezone, timedelta
from collections import defaultdict

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
    log.warning('PyFlink 未安装，以 Python 模拟模式运行（等价功能，适用于开发/容器环境）')


# ── ClickHouse 连接 ───────────────────────────────────────────

def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


# ══════════════════════════════════════════════════════════════
# PyFlink Table API（生产集群模式）
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
    'properties.group.id'           = '{consumer_group}',
    'scan.startup.mode'             = '{startup_mode}',
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
    'properties.group.id'           = '{consumer_group}_payments',
    'scan.startup.mode'             = '{startup_mode}',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true'
)
"""

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


def run_flink_job(startup_mode: str = 'latest-offset'):
    log.info('PyFlink Table API 启动，startup_mode=%s', startup_mode)
    consumer_group = (
        f'flink_replay_{int(time.time())}'
        if startup_mode == 'earliest'
        else 'flink_realtime'
    )

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
        consumer_group=consumer_group,
        startup_mode=startup_mode,
    )

    for ddl in [
        FLINK_KAFKA_ORDERS_DDL.format(**params),
        FLINK_KAFKA_PAYMENTS_DDL.format(**params),
        FLINK_SINK_MINUTE_STATS_DDL.format(**params),
        FLINK_SINK_DWD_DDL.format(**params),
    ]:
        t_env.execute_sql(ddl)

    stats_job = t_env.execute_sql(FLINK_MINUTE_AGG_SQL)
    dwd_job   = t_env.execute_sql(FLINK_DWD_JOIN_SQL)
    log.info('Flink 作业已提交（%s 模式）', startup_mode)


# ══════════════════════════════════════════════════════════════
# Python 模拟模式（开发/容器环境）
# ══════════════════════════════════════════════════════════════

def _write_replay_job(ch, job_id: str, job_name: str, status: str,
                      records: int = 0, error: str = '', end: bool = False):
    row = [[
        job_id, job_name, 'manual', 'earliest', None, None,
        datetime.now(), datetime.now() if end else None,
        records, status, error, '',
    ]]
    try:
        ch.insert(
            'stream.kappa_replay_jobs', row,
            column_names=['job_id', 'job_name', 'triggered_by', 'from_offset',
                          'replay_from_time', 'replay_until_time',
                          'start_time', 'end_time', 'records_processed',
                          'status', 'error_msg', 'notes'],
        )
    except Exception as e:
        log.error('写 replay job 失败：%s', e)


def _flush_window_realtime(ch, data: list, w_start: datetime):
    """实时模式：1分钟窗口 → dws.realtime_minute_stats + 告警检测"""
    if not data:
        return
    w_end = w_start + timedelta(minutes=1)
    order_cnt        = len(data)
    total_gmv        = round(sum(d.get('price', 0) for d in data), 2)
    avg_price        = round(total_gmv / order_cnt, 2) if order_cnt else 0
    unique_customers = len({d.get('customer_id') for d in data})
    cat_counter      = defaultdict(int)
    for d in data:
        cat_counter[d.get('product_category', '')] += 1
    top_category = max(cat_counter, key=cat_counter.get) if cat_counter else ''

    try:
        ch.insert(
            'dws.realtime_minute_stats',
            [[w_start.replace(tzinfo=None), w_end.replace(tzinfo=None),
              order_cnt, total_gmv, avg_price, unique_customers, top_category,
              datetime.now()]],
            column_names=['window_start', 'window_end', 'order_cnt', 'total_gmv',
                          'avg_price', 'unique_customers', 'top_category', '_created_at'],
        )
        log.info('[实时窗口 %s] %d 单，GMV=%.0f，均价=%.2f',
                 w_start.strftime('%H:%M'), order_cnt, total_gmv, avg_price)
    except Exception as e:
        log.error('写入 realtime_minute_stats 失败：%s', e)

    _ai_quality_gate(ch, data, order_cnt, total_gmv, avg_price, w_start, w_end)


def _flush_window_replay(ch, data: list, w_start: datetime, job_id: str) -> int:
    """回放模式：按小时聚合 → dws.kappa_hourly_agg（幂等写入）"""
    if not data:
        return 0
    hour_start = w_start.replace(minute=0, second=0, microsecond=0, tzinfo=None)

    # 按品类+州分组聚合
    groups: dict = defaultdict(lambda: {'orders': 0, 'gmv': 0.0, 'canceled': 0, 'customers': set()})
    for d in data:
        key = (d.get('product_category', ''), d.get('state', ''))
        g = groups[key]
        g['orders'] += 1
        g['gmv'] += d.get('price', 0)
        if d.get('order_status') == 'canceled':
            g['canceled'] += 1
        g['customers'].add(d.get('customer_id', ''))

    rows = []
    for (cat, state), g in groups.items():
        cnt = g['orders']
        gmv = round(g['gmv'], 2)
        rows.append([
            hour_start, cat, state, cnt,
            gmv, round(gmv / cnt, 2) if cnt else 0,
            g['canceled'], len(g['customers']),
            job_id, datetime.now(),
        ])

    try:
        ch.insert(
            'dws.kappa_hourly_agg', rows,
            column_names=['hour_start', 'product_category', 'state', 'order_cnt',
                          'total_gmv', 'avg_price', 'cancel_cnt', 'unique_customers',
                          'replay_job_id', '_updated_at'],
        )
    except Exception as e:
        log.error('写入 kappa_hourly_agg 失败：%s', e)
    return len(data)


def _ai_quality_gate(ch, data: list, order_cnt: int, total_gmv: float,
                     avg_price: float, w_start: datetime, w_end: datetime):
    """AI 质量门控：规则检测 + LLM 告警分析"""
    alerts = []

    canceled = sum(1 for d in data if d.get('order_status') == 'canceled')
    if order_cnt > 5 and canceled / order_cnt > 0.15:
        alerts.append({
            'alert_type': 'QUALITY', 'severity': 'HIGH',
            'field_name': 'order_status',
            'detail': f"取消率异常：{canceled/order_cnt:.1%}（{canceled}/{order_cnt} 单）",
            'metric_value': float(canceled / order_cnt),
            'threshold_value': 0.15,
        })

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

    try:
        from openai import OpenAI
        llm = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=15.0)
        desc = '\n'.join(f'- {a["detail"]}' for a in alerts)
        prompt = (
            f"时间窗口 {w_start.strftime('%H:%M')}，{order_cnt}单，GMV R${total_gmv:.0f}。\n"
            f"异常：\n{desc}\n\n"
            "针对每条异常给出1句原因推断和1句处理建议，中文，每条不超过30字。"
        )
        resp = llm.chat.completions.create(
            model=cfg.llm_model,
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.3, max_tokens=200,
        )
        lines = resp.choices[0].message.content.strip().split('\n')
        for i, a in enumerate(alerts):
            a['ai_suggestion'] = lines[i] if i < len(lines) else lines[0]
    except Exception as e:
        for a in alerts:
            a['ai_suggestion'] = f'AI分析不可用：{str(e)[:50]}'

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
        log.warning('[AI质检] 写入 %d 条告警', len(alerts))
    except Exception as e:
        log.error('写入告警失败：%s', e)


def _write_lag(ch, consumer_group: str, topic: str, is_replay: int,
               current_offset: int, log_end: int):
    try:
        lag = max(0, log_end - current_offset)
        ch.insert(
            'stream.kappa_consumer_lag',
            [[datetime.now(), consumer_group, topic, 0,
              current_offset, log_end, lag, is_replay, 0.0]],
            column_names=['check_time', 'consumer_group', 'topic', 'partition_id',
                          'current_offset', 'log_end_offset', 'lag',
                          'is_replay', 'throughput_per_s'],
        )
    except Exception:
        pass


def run_python_realtime():
    """实时消费模式：消费 Kafka 最新数据，1分钟窗口聚合"""
    from kafka import KafkaConsumer

    log.info('Kappa 实时模式启动（Python 模拟）')
    consumer = KafkaConsumer(
        cfg.orders_topic,
        bootstrap_servers=cfg.kafka_bootstrap,
        group_id='kappa_realtime',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=1000,
    )
    ch = _get_ch()

    window_data: list = []
    window_start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    lag_report_at = time.time()

    try:
        while True:
            now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            if now > window_start:
                _flush_window_realtime(ch, window_data, window_start)
                window_data = []
                window_start = now

            batch = consumer.poll(timeout_ms=500, max_records=1000)
            for _, msgs in batch.items():
                for msg in msgs:
                    if msg.value:
                        window_data.append(msg.value)

            # 每30秒上报 consumer lag
            if time.time() - lag_report_at > 30:
                try:
                    partitions = consumer.partitions_for_topic(cfg.orders_topic) or set()
                    from kafka import TopicPartition
                    tps = [TopicPartition(cfg.orders_topic, p) for p in partitions]
                    end_offsets = consumer.end_offsets(tps)
                    pos = {tp: consumer.position(tp) for tp in tps}
                    total_lag = sum(max(0, end_offsets[tp] - pos[tp]) for tp in tps)
                    total_end = sum(end_offsets[tp] for tp in tps)
                    total_pos = sum(pos[tp] for tp in tps)
                    _write_lag(ch, 'kappa_realtime', cfg.orders_topic, 0, total_pos, total_end)
                except Exception:
                    pass
                lag_report_at = time.time()

            time.sleep(0.1)

    except KeyboardInterrupt:
        log.info('实时处理停止')
        _flush_window_realtime(ch, window_data, window_start)
    finally:
        consumer.close()


def run_python_replay(job_name: str = None):
    """
    Kappa 回放模式：从 Kafka earliest offset 消费全量历史数据，
    按小时维度聚合后写入 dws.kappa_hourly_agg（幂等，可多次执行）。
    这是 Kappa 架构的核心能力：历史数据重算无需独立批处理管道。
    """
    from kafka import KafkaConsumer

    job_id   = str(uuid.uuid4())
    job_name = job_name or f'full_replay_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
    log.info('Kappa 回放模式启动：job_id=%s', job_id)

    ch = _get_ch()
    _write_replay_job(ch, job_id, job_name, 'running')

    consumer = KafkaConsumer(
        cfg.orders_topic,
        bootstrap_servers=cfg.kafka_bootstrap,
        group_id=f'kappa_replay_{job_id[:8]}',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        enable_auto_commit=True,
    )

    # 按小时桶聚合，定期 flush
    hour_buckets: dict = defaultdict(list)
    total_processed = 0
    last_flush_time = time.time()

    try:
        log.info('开始消费 Kafka 历史数据（offset=earliest）...')
        for msg in consumer:
            if not msg.value:
                continue
            row = msg.value
            try:
                ts_str = row.get('event_time', '')
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00')) if ts_str else datetime.now()
            except ValueError:
                ts = datetime.now()

            hour_key = ts.replace(minute=0, second=0, microsecond=0, tzinfo=None)
            hour_buckets[hour_key].append(row)
            total_processed += 1

            # 每5万条或每60秒 flush 一次已完整的小时桶
            if total_processed % 50000 == 0 or (time.time() - last_flush_time > 60):
                cutoff = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
                flushed_hours = [h for h in list(hour_buckets.keys()) if h < cutoff]
                for h in flushed_hours:
                    _flush_window_replay(ch, hour_buckets.pop(h), h, job_id)
                if total_processed % 50000 == 0:
                    log.info('[回放] 已处理 %d 条，活跃小时桶 %d 个', total_processed, len(hour_buckets))
                last_flush_time = time.time()

    except StopIteration:
        log.info('[回放] Kafka 消费完毕')
    except KeyboardInterrupt:
        log.info('[回放] 手动停止')
    finally:
        # flush 所有剩余桶
        for h, data in hour_buckets.items():
            _flush_window_replay(ch, data, h, job_id)
        consumer.close()

    log.info('[回放] 完成：总计 %d 条，job_id=%s', total_processed, job_id)
    _write_replay_job(ch, job_id, job_name, 'completed', total_processed, end=True)


# ── 主入口 ────────────────────────────────────────────────────

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Kappa 架构 Flink/Python 统一流处理')
    parser.add_argument('--mode', choices=['flink', 'python', 'auto'], default='auto',
                        help='运行模式：flink=PyFlink集群, python=纯Python, auto=自动选择')
    parser.add_argument('--replay', action='store_true',
                        help='启用 Kappa 回放模式（从 earliest offset 重算历史）')
    parser.add_argument('--job-name', default=None,
                        help='回放任务名称（默认自动生成）')
    args = parser.parse_args()

    use_flink = (args.mode == 'flink') or (args.mode == 'auto' and PYFLINK_AVAILABLE)

    if args.replay:
        if use_flink:
            run_flink_job(startup_mode='earliest')
        else:
            run_python_replay(job_name=args.job_name)
    else:
        if use_flink:
            run_flink_job(startup_mode='latest-offset')
        else:
            run_python_realtime()
