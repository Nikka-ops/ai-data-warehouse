-- ============================================================
-- Flink 实时处理输出表配置
-- Flink 将结果写入专用 Kafka Topics → ClickHouse Kafka Engine 消费
-- ============================================================

CREATE DATABASE IF NOT EXISTS stream;

-- ============================================================
-- STEP 1：Flink 输出 Kafka Engine 表（消费入口）
-- ============================================================

-- Flink 输出：1分钟窗口聚合统计
CREATE TABLE IF NOT EXISTS stream.kafka_flink_minute_stats (
    window_start     String,
    window_end       String,
    order_cnt        UInt64,
    total_gmv        Float64,
    avg_price        Float64,
    unique_customers UInt64,
    top_category     String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:29092',
    kafka_topic_list    = 'flink.minute_stats',
    kafka_group_name    = 'clickhouse_flink_stats',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 5;


-- Flink 输出：实时 DWD 宽表（订单+支付关联）
CREATE TABLE IF NOT EXISTS stream.kafka_flink_realtime_dwd (
    order_id         String,
    customer_id      String,
    product_id       String,
    product_category String,
    seller_id        String,
    state            String,
    city             String,
    price            Float64,
    freight_value    Float64,
    total_amount     Float64,
    payment_type     String,
    payment_value    Float64,
    order_status     String,
    event_time       String,
    event_date       String,
    event_hour       UInt8,
    is_paid          UInt8
) ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:29092',
    kafka_topic_list    = 'flink.realtime_dwd',
    kafka_group_name    = 'clickhouse_flink_dwd',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 5;


-- ============================================================
-- STEP 2：物化视图（Flink Kafka → 落地表）
-- ============================================================

-- Flink 分钟统计 → dws.realtime_minute_stats
CREATE MATERIALIZED VIEW IF NOT EXISTS stream.mv_flink_stats_to_dws
TO dws.realtime_minute_stats
AS SELECT
    parseDateTimeBestEffortOrNull(window_start)  AS window_start,
    parseDateTimeBestEffortOrNull(window_end)    AS window_end,
    order_cnt,
    total_gmv,
    avg_price,
    unique_customers,
    top_category,
    now()                                        AS _created_at
FROM stream.kafka_flink_minute_stats
WHERE isNotNull(parseDateTimeBestEffortOrNull(window_start));


-- Flink DWD → dwd.realtime_order_detail
CREATE MATERIALIZED VIEW IF NOT EXISTS stream.mv_flink_dwd
TO dwd.realtime_order_detail
AS SELECT
    order_id,
    customer_id,
    product_id,
    product_category,
    seller_id,
    state,
    city,
    price,
    freight_value,
    total_amount,
    payment_type,
    payment_value,
    order_status,
    parseDateTimeBestEffortOrNull(event_time)    AS event_time,
    toDate(parseDateTimeBestEffortOrNull(event_time)) AS event_date,
    event_hour,
    is_paid,
    now()                                        AS _ingest_time
FROM stream.kafka_flink_realtime_dwd
WHERE isNotNull(parseDateTimeBestEffortOrNull(event_time));


-- ============================================================
-- STEP 3：实时 ADS 视图（NL2SQL 可直接查询）
-- ============================================================

-- 实时小时汇总视图（供 NL2SQL 查询今日小时趋势）
CREATE VIEW IF NOT EXISTS ads.realtime_hourly AS
SELECT
    toStartOfHour(event_time)       AS hour_start,
    count(DISTINCT order_id)        AS order_cnt,
    round(sum(price), 2)            AS gmv,
    round(avg(price), 2)            AS avg_price,
    count(DISTINCT customer_id)     AS unique_customers,
    countIf(order_status='canceled') AS cancel_cnt
FROM ods.orders_stream
WHERE event_time >= today()
GROUP BY hour_start
ORDER BY hour_start;


-- 实时品类 Top 视图（今日）
CREATE VIEW IF NOT EXISTS ads.realtime_category_today AS
SELECT
    product_category,
    count(DISTINCT order_id)    AS order_cnt,
    round(sum(price), 2)        AS gmv,
    round(avg(price), 2)        AS avg_price
FROM ods.orders_stream
WHERE event_time >= today()
GROUP BY product_category
ORDER BY gmv DESC;


-- 实时州销售视图（今日）
CREATE VIEW IF NOT EXISTS ads.realtime_state_today AS
SELECT
    state,
    count(DISTINCT order_id)    AS order_cnt,
    round(sum(price), 2)        AS gmv,
    rank() OVER (ORDER BY sum(price) DESC) AS rank_by_gmv
FROM ods.orders_stream
WHERE event_time >= today()
GROUP BY state;
