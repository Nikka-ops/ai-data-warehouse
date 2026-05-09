-- ============================================================
-- ClickHouse 流式接入配置
-- Kafka 引擎 + 物化视图 → 自动消费 Kafka 写入 ODS 表
-- ============================================================

-- 确保数据库存在
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dws;
CREATE DATABASE IF NOT EXISTS ads;
CREATE DATABASE IF NOT EXISTS stream;   -- 流式处理专用库


-- ============================================================
-- STEP 1：Kafka 引擎表（只读，不存数据，作为消费入口）
-- ============================================================

-- 订单消息流
CREATE TABLE IF NOT EXISTS stream.kafka_orders (
    order_id         String,
    customer_id      String,
    product_id       String,
    product_category String,
    seller_id        String,
    price            Float64,
    freight_value    Float64,
    order_status     String,
    state            String,
    city             String,
    event_time       String,    -- Kafka 消息里的字符串时间，后续转换
    msg_version      String     -- 消息版本号，用于幂等
) ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'kafka:29092',
    kafka_topic_list     = 'orders_stream',
    kafka_group_name     = 'clickhouse_orders_consumer',
    kafka_format         = 'JSONEachRow',
    kafka_num_consumers  = 1,
    kafka_skip_broken_messages = 10;   -- 跳过格式错误的消息

-- 支付消息流
CREATE TABLE IF NOT EXISTS stream.kafka_payments (
    payment_id       String,
    order_id         String,
    payment_type     String,
    payment_value    Float64,
    installments     UInt8,
    event_time       String,
    msg_version      String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list  = 'kafka:29092',
    kafka_topic_list   = 'payments_stream',
    kafka_group_name   = 'clickhouse_payments_consumer',
    kafka_format       = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;


-- ============================================================
-- STEP 2：ODS 落地表（真正存数据）
-- ============================================================

CREATE TABLE IF NOT EXISTS ods.orders_stream (
    order_id         String,
    customer_id      String,
    product_id       String,
    product_category String,
    seller_id        String,
    price            Float64,
    freight_value    Float64,
    order_status     String,
    state            String,
    city             String,
    event_time       DateTime,
    _kafka_offset    UInt64,
    _ingest_time     DateTime DEFAULT now(),
    _load_date       Date     DEFAULT today()
) ENGINE = ReplacingMergeTree(_ingest_time)
PARTITION BY toYYYYMMDD(_load_date)
ORDER BY (order_id, event_time)
COMMENT '实时订单流 ODS 落地表';

CREATE TABLE IF NOT EXISTS ods.payments_stream (
    payment_id       String,
    order_id         String,
    payment_type     LowCardinality(String),
    payment_value    Float64,
    installments     UInt8,
    event_time       DateTime,
    _ingest_time     DateTime DEFAULT now(),
    _load_date       Date     DEFAULT today()
) ENGINE = ReplacingMergeTree(_ingest_time)
PARTITION BY toYYYYMMDD(_load_date)
ORDER BY (payment_id, event_time)
COMMENT '实时支付流 ODS 落地表';


-- ============================================================
-- STEP 3：物化视图（自动触发：Kafka有数据 → 写ODS落地表）
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS stream.mv_orders_to_ods
TO ods.orders_stream
AS SELECT
    order_id,
    customer_id,
    product_id,
    product_category,
    seller_id,
    price,
    freight_value,
    order_status,
    state,
    city,
    parseDateTimeBestEffortOrNull(event_time) AS event_time,
    rowNumberInAllBlocks()                     AS _kafka_offset,
    now()                                      AS _ingest_time,
    today()                                    AS _load_date
FROM stream.kafka_orders
WHERE isNotNull(parseDateTimeBestEffortOrNull(event_time))
  AND price >= 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS stream.mv_payments_to_ods
TO ods.payments_stream
AS SELECT
    payment_id,
    order_id,
    payment_type,
    payment_value,
    installments,
    parseDateTimeBestEffortOrNull(event_time) AS event_time,
    now()                                      AS _ingest_time,
    today()                                    AS _load_date
FROM stream.kafka_payments
WHERE isNotNull(parseDateTimeBestEffortOrNull(event_time))
  AND payment_value >= 0;


-- ============================================================
-- STEP 4：实时 DWD 宽表（物化视图自动聚合）
-- ============================================================

CREATE TABLE IF NOT EXISTS dwd.realtime_order_detail (
    order_id         String,
    customer_id      String,
    product_id       String,
    product_category String,
    state            String,
    city             String,
    price            Float64,
    freight_value    Float64,
    total_amount     Float64,
    payment_type     LowCardinality(String),
    payment_value    Float64,
    order_status     String,
    event_time       DateTime,
    event_date       Date,
    event_hour       UInt8,
    is_paid          UInt8,
    _ingest_time     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ingest_time)
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (order_id, event_time)
COMMENT '实时订单支付宽表 DWD';

-- 物化视图：订单 + 支付自动关联写入 DWD
-- 注意：此处只做订单侧的写入，支付数据通过 JOIN 查询实时关联
CREATE MATERIALIZED VIEW IF NOT EXISTS stream.mv_orders_to_dwd
TO dwd.realtime_order_detail
AS SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.product_category,
    o.state,
    o.city,
    o.price,
    o.freight_value,
    o.price + o.freight_value                AS total_amount,
    ''                                        AS payment_type,   -- 支付数据异步关联
    0.0                                       AS payment_value,
    o.order_status,
    o.event_time,
    toDate(o.event_time)                      AS event_date,
    toHour(o.event_time)                      AS event_hour,
    if(o.order_status = 'delivered', 1, 0)   AS is_paid,
    now()                                     AS _ingest_time
FROM stream.kafka_orders o
WHERE isNotNull(o.event_time)
  AND o.price >= 0;


-- ============================================================
-- STEP 5：实时聚合表（分钟级 DWS）
-- ============================================================

CREATE TABLE IF NOT EXISTS dws.realtime_minute_stats (
    window_start     DateTime,
    window_end       DateTime,
    order_cnt        UInt64,
    total_gmv        Float64,
    avg_price        Float64,
    unique_customers UInt64,
    top_category     String,
    _created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY window_start
TTL window_start + INTERVAL 7 DAY    -- 7天后自动删除明细
COMMENT '实时分钟级聚合统计';


-- ============================================================
-- STEP 6：AI 质检结果表（存储实时质检告警）
-- ============================================================

CREATE TABLE IF NOT EXISTS stream.ai_quality_alerts (
    alert_id         String DEFAULT generateUUIDv4(),
    alert_time       DateTime DEFAULT now(),
    alert_type       String,      -- 'ANOMALY' / 'QUALITY' / 'PATTERN'
    severity         String,      -- 'HIGH' / 'MEDIUM' / 'LOW'
    table_name       String,
    field_name       String,
    detail           String,
    ai_suggestion    String,
    window_start     DateTime,
    window_end       DateTime,
    metric_value     Float64,
    threshold_value  Float64
) ENGINE = MergeTree()
ORDER BY (alert_time, severity)
TTL alert_time + INTERVAL 30 DAY
COMMENT 'AI 实时质检告警表';
