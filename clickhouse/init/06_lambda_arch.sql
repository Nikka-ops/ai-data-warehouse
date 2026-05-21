-- ============================================================
-- Lambda 架构：离线批处理层 + 实时速度层 + 合并服务层
-- ============================================================

-- ── 离线层：历史订单 ODS（批量加载，不经过 Kafka）──────────
CREATE TABLE IF NOT EXISTS ods.orders_batch (
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
    event_date       Date,
    _batch_id        String,           -- 批次标识（YYYYMMDD）
    _load_time       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY toYYYYMM(event_date)
ORDER BY (order_id, event_time)
COMMENT '历史订单批量加载表（Lambda 离线层）';

CREATE TABLE IF NOT EXISTS ods.payments_batch (
    payment_id    String,
    order_id      String,
    payment_type  LowCardinality(String),
    payment_value Float64,
    installments  UInt8,
    event_date    Date,
    _batch_id     String,
    _load_time    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY toYYYYMM(event_date)
ORDER BY (payment_id, order_id)
COMMENT '历史支付批量加载表（Lambda 离线层）';

-- ── 离线层：批处理日级汇总（批量计算结果）──────────────────
CREATE TABLE IF NOT EXISTS dws.batch_daily_stats (
    stat_date        Date,
    product_category String,
    state            String,
    order_cnt        UInt64,
    total_gmv        Float64,
    avg_price        Float64,
    cancel_cnt       UInt64,
    unique_customers UInt64,
    unique_sellers   UInt64,
    _batch_id        String,
    _created_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_created_at)
PARTITION BY toYYYYMM(stat_date)
ORDER BY (stat_date, product_category, state)
COMMENT '批处理日级汇总（Lambda 离线层结果）';

-- ── 服务层：Lambda 合并视图（批处理历史 + 今日实时）─────────
-- 日级视图：可直接在 Superset 中用于历史趋势分析
CREATE VIEW IF NOT EXISTS dws.serving_daily AS
SELECT
    stat_date                                AS day,
    sum(order_cnt)                           AS order_cnt,
    round(sum(total_gmv), 2)                 AS total_gmv,
    round(sum(total_gmv) / sum(order_cnt), 2) AS avg_price,
    sum(cancel_cnt)                          AS cancel_cnt,
    sum(unique_customers)                    AS unique_customers,
    'batch'                                  AS source
FROM dws.batch_daily_stats
WHERE stat_date < today()
GROUP BY stat_date
UNION ALL
SELECT
    toDate(window_start)               AS day,
    sum(order_cnt)                     AS order_cnt,
    round(sum(total_gmv), 2)           AS total_gmv,
    round(avg(avg_price), 2)           AS avg_price,
    0                                  AS cancel_cnt,
    sum(unique_customers)              AS unique_customers,
    'realtime'                         AS source
FROM dws.realtime_minute_stats
WHERE window_start >= today()
GROUP BY day;

-- 品类维度服务层（批处理历史 + 今日实时）
CREATE VIEW IF NOT EXISTS dws.serving_category AS
SELECT
    stat_date               AS day,
    product_category,
    sum(order_cnt)          AS order_cnt,
    round(sum(total_gmv), 2) AS total_gmv,
    round(avg(avg_price), 2) AS avg_price,
    sum(cancel_cnt)         AS cancel_cnt,
    'batch'                 AS source
FROM dws.batch_daily_stats
WHERE stat_date < today()
GROUP BY stat_date, product_category
UNION ALL
SELECT
    today()                         AS day,
    product_category,
    count(DISTINCT order_id)        AS order_cnt,
    round(sum(price), 2)            AS total_gmv,
    round(avg(price), 2)            AS avg_price,
    countIf(order_status='canceled') AS cancel_cnt,
    'realtime'                      AS source
FROM ods.orders_stream
WHERE event_time >= today()
GROUP BY product_category;

-- ── 数据一致性校验表（Lambda 双层对账）──────────────────────
CREATE TABLE IF NOT EXISTS stream.lambda_reconciliation (
    check_time        DateTime DEFAULT now(),
    check_date        Date,
    batch_order_cnt   UInt64,
    stream_order_cnt  UInt64,
    batch_gmv         Float64,
    stream_gmv        Float64,
    cnt_diff_pct      Float64,    -- 计数差异百分比
    gmv_diff_pct      Float64,    -- GMV差异百分比
    is_consistent     UInt8,      -- 1=一致（差异<2%），0=不一致
    check_status      String      -- 'OK' / 'WARN' / 'MISMATCH'
) ENGINE = MergeTree()
ORDER BY (check_time, check_date)
TTL check_time + INTERVAL 30 DAY
COMMENT 'Lambda 架构批实时数据一致性对账记录';
