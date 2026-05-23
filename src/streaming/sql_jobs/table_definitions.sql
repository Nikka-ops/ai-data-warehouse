-- Flink SQL：Kafka Source 表定义

CREATE TABLE IF NOT EXISTS orders_source (
    order_id      STRING,
    customer_id   STRING,
    seller_id     STRING,
    category      STRING,
    price         DOUBLE,
    quantity      INT,
    state         STRING,
    event_time    TIMESTAMP(3),
    proc_time     AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders_stream',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-sql-group',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE IF NOT EXISTS payments_source (
    payment_id    STRING,
    order_id      STRING,
    payment_type  STRING,
    amount        DOUBLE,
    status        STRING,
    event_time    TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'payments_stream',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- ClickHouse Sink 表
CREATE TABLE IF NOT EXISTS minute_stats_sink (
    window_start  TIMESTAMP(3),
    category      STRING,
    order_cnt     BIGINT,
    total_gmv     DOUBLE,
    avg_price     DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:clickhouse://${CLICKHOUSE_HOST}:8123/dws',
    'table-name' = 'realtime_minute_stats',
    'username' = '${CLICKHOUSE_USER}',
    'password' = '${CLICKHOUSE_PASSWORD}'
);
