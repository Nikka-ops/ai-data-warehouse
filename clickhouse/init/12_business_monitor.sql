-- =============================================================================
-- 业务监控层：业务指标告警 + 慢查询诊断
-- =============================================================================

-- 业务告警表
CREATE TABLE IF NOT EXISTS stream.business_alerts
(
    alert_id       UUID    DEFAULT generateUUIDv4(),
    alert_time     DateTime DEFAULT now(),
    metric_name    String,
    current_value  Float64,
    baseline_value Float64,
    change_pct     Float64,
    severity       String,   -- 'HIGH' | 'CRITICAL'
    detail         String,
    root_cause     String,   -- LLM 生成
    webhook_sent   UInt8 DEFAULT 0,
    resolved       UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(alert_time)
ORDER BY (alert_time, metric_name)
TTL alert_time + INTERVAL 90 DAY;

-- 未处理告警视图
CREATE VIEW IF NOT EXISTS stream.business_alerts_active AS
SELECT *
FROM stream.business_alerts
WHERE resolved = 0
ORDER BY alert_time DESC;

-- 告警统计视图（供 dashboard 使用）
CREATE VIEW IF NOT EXISTS stream.business_alerts_stats AS
SELECT
    toDate(alert_time)      AS alert_date,
    metric_name,
    severity,
    count()                 AS alert_count,
    countIf(resolved = 1)   AS resolved_count
FROM stream.business_alerts
WHERE alert_time >= now() - INTERVAL 7 DAY
GROUP BY alert_date, metric_name, severity
ORDER BY alert_date DESC, alert_count DESC;

-- 慢查询分析表
CREATE TABLE IF NOT EXISTS stream.slow_query_analysis
(
    analyzed_at  DateTime DEFAULT now(),
    query_time   DateTime,
    duration_ms  UInt64,
    query_sql    String,
    read_rows    UInt64,
    read_bytes   UInt64,
    suggestion   String,  -- LLM 生成的优化建议
    category     String   -- 'MISSING_INDEX'|'FULL_SCAN'|'INEFFICIENT_JOIN'|'OTHER'
) ENGINE = ReplacingMergeTree(analyzed_at)
ORDER BY (query_time, query_sql)
TTL analyzed_at + INTERVAL 30 DAY;

-- 慢查询汇总视图
CREATE VIEW IF NOT EXISTS stream.slow_query_summary AS
SELECT
    category,
    count()            AS query_count,
    avg(duration_ms)   AS avg_duration_ms,
    max(duration_ms)   AS max_duration_ms,
    sum(read_rows)     AS total_read_rows
FROM stream.slow_query_analysis
WHERE analyzed_at >= now() - INTERVAL 7 DAY
GROUP BY category
ORDER BY query_count DESC;
