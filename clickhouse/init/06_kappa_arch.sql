-- ============================================================
-- Kappa 架构：Kafka 可回放日志 + Flink 统一流处理 + ClickHouse 服务层
-- 替代 Lambda 架构：无独立批处理层，历史重算通过 Flink 回放 Kafka 完成
-- ============================================================

-- ── Flink 重放任务跟踪表 ──────────────────────────────────────
-- 记录每次历史重算任务（全量回放 / 指定时间段回放）的状态
CREATE TABLE IF NOT EXISTS stream.kappa_replay_jobs (
    job_id            String DEFAULT generateUUIDv4(),
    job_name          String,                              -- 任务名称（如 full_replay_20240101）
    triggered_by      String DEFAULT 'manual',             -- manual / ai_agent / schedule
    from_offset       String DEFAULT 'earliest',           -- earliest / latest / 具体 offset
    replay_from_time  Nullable(DateTime),                  -- 按时间过滤起点（NULL=全量）
    replay_until_time Nullable(DateTime),                  -- 按时间过滤终点（NULL=最新）
    start_time        DateTime DEFAULT now(),
    end_time          Nullable(DateTime),
    records_processed UInt64 DEFAULT 0,
    status            LowCardinality(String) DEFAULT 'running',  -- running/completed/failed/cancelled
    error_msg         String DEFAULT '',
    notes             String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY start_time
TTL start_time + INTERVAL 90 DAY
COMMENT 'Kappa 架构 Flink 历史回放任务记录';

-- ── Kafka 消费者 Lag 监控 ─────────────────────────────────────
-- Flink 每次 checkpoint 后写入，用于监控实时处理进度和回放进度
CREATE TABLE IF NOT EXISTS stream.kappa_consumer_lag (
    check_time       DateTime DEFAULT now(),
    consumer_group   LowCardinality(String),
    topic            LowCardinality(String),
    partition_id     UInt8,
    current_offset   UInt64,
    log_end_offset   UInt64,
    lag              UInt64,                               -- 剩余未消费消息数
    is_replay        UInt8 DEFAULT 0,                      -- 1=正在重放，0=正常实时
    throughput_per_s Float64 DEFAULT 0                     -- 消费速率（条/秒）
) ENGINE = ReplacingMergeTree(check_time)
PARTITION BY toYYYYMM(check_time)
ORDER BY (consumer_group, topic, partition_id)
TTL check_time + INTERVAL 7 DAY
COMMENT 'Kafka 消费者 Lag 监控（实时 + 回放进度追踪）';

-- ── Kappa 小时级历史存储（Flink 回放输出落地）────────────────
-- 替代 Lambda 离线批处理结果，由 Flink 回放 Kafka 后聚合写入
-- ReplacingMergeTree 保证重放幂等：相同 (hour_start, product_category, state) 自动去重
CREATE TABLE IF NOT EXISTS dws.kappa_hourly_agg (
    hour_start       DateTime,
    product_category LowCardinality(String),
    state            LowCardinality(String),
    order_cnt        UInt64,
    total_gmv        Float64,
    avg_price        Float64,
    cancel_cnt       UInt64,
    unique_customers UInt64,
    replay_job_id    String DEFAULT '',                    -- 关联 kappa_replay_jobs.job_id
    _updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(hour_start)
ORDER BY (hour_start, product_category, state)
COMMENT 'Kappa 架构小时级聚合（Flink 回放 Kafka 后写入，可幂等重算）';

-- ── Kappa 服务层统一视图 ──────────────────────────────────────
-- 规则：优先使用已回放的历史聚合（kappa_hourly_agg）；
--       最近2小时实时分钟统计作为最新补充（两者不重叠）
CREATE VIEW IF NOT EXISTS dws.kappa_serving_unified AS
WITH latest_replay AS (
    SELECT max(hour_start) AS replay_until
    FROM dws.kappa_hourly_agg
)
SELECT
    hour_start,
    product_category,
    state,
    order_cnt,
    round(total_gmv, 2)  AS total_gmv,
    round(avg_price, 2)  AS avg_price,
    cancel_cnt,
    unique_customers,
    'kappa_replay'       AS source
FROM dws.kappa_hourly_agg
WHERE hour_start <= (SELECT replay_until FROM latest_replay)

UNION ALL

SELECT
    toStartOfHour(window_start)   AS hour_start,
    ''                            AS product_category,
    ''                            AS state,
    sum(order_cnt)                AS order_cnt,
    round(sum(total_gmv), 2)      AS total_gmv,
    round(avg(avg_price), 2)      AS avg_price,
    0                             AS cancel_cnt,
    sum(unique_customers)         AS unique_customers,
    'realtime'                    AS source
FROM dws.realtime_minute_stats
WHERE window_start > (SELECT replay_until FROM latest_replay)
GROUP BY hour_start;

-- ── 当前 GMV 实时汇总（API / Superset 直接调用）──────────────
CREATE VIEW IF NOT EXISTS ads.kappa_current_totals AS
SELECT
    sum(total_gmv)              AS total_gmv,
    sum(order_cnt)              AS total_orders,
    count(DISTINCT hour_start)  AS hours_covered,
    min(hour_start)             AS data_from,
    max(hour_start)             AS data_until
FROM dws.kappa_serving_unified;
