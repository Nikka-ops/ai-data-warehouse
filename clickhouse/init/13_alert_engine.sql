-- =============================================================================
-- 告警引擎数据层
-- stream.alert_events      : 告警事件归档（所有触发的告警）
-- stream.agent_decision_log: Agent 决策 + 技能执行日志
-- stream.alert_silence_rules: 静默规则配置
-- =============================================================================

-- 告警事件归档表（全量持久化）
CREATE TABLE IF NOT EXISTS stream.alert_events
(
    alert_id          String,
    fired_at          DateTime DEFAULT now(),
    source            String,   -- rule_engine / anomaly_detector / trend_predictor / lineage_impact
    category          String,   -- DATA_QUALITY / SYSTEM / BUSINESS / CAPACITY
    severity          String,   -- P1 / P2 / P3 / P4
    title             String,
    detail            String,
    metric_name       String,
    current_value     Float64,
    threshold_value   Float64,
    affected_tables   Array(String),
    downstream_tables Array(String),
    fingerprint       String,
    context_json      String DEFAULT '{}'
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(fired_at)
ORDER BY (fired_at, severity, source)
TTL fired_at + INTERVAL 180 DAY;

-- Agent 决策与技能执行日志
CREATE TABLE IF NOT EXISTS stream.agent_decision_log
(
    log_id         UUID    DEFAULT generateUUIDv4(),
    log_time       DateTime DEFAULT now(),
    alert_id       String,
    alert_title    String,
    alert_severity String,
    skill_name     String,
    action_type    String,
    target         String,
    risk_level     String,   -- low / medium / high / critical
    dry_run        UInt8 DEFAULT 1,
    allowed        UInt8 DEFAULT 0,
    success        UInt8 DEFAULT 0,
    message        String DEFAULT '',
    resolution     String DEFAULT '',
    resolved       UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_time)
ORDER BY log_time
TTL log_time + INTERVAL 90 DAY;

-- 静默规则配置表（运行时可动态插入）
CREATE TABLE IF NOT EXISTS stream.alert_silence_rules
(
    rule_id       UUID    DEFAULT generateUUIDv4(),
    created_at    DateTime DEFAULT now(),
    source        String DEFAULT '',    -- 空=匹配所有来源
    metric_name   String DEFAULT '',    -- 空=匹配所有指标
    expires_at    DateTime,
    reason        String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY created_at
TTL expires_at + INTERVAL 1 DAY;

-- ── 统计视图 ──────────────────────────────────────────────────

-- 最近24小时告警趋势（按小时+严重度）
CREATE VIEW IF NOT EXISTS stream.alert_hourly_trend AS
SELECT
    toStartOfHour(fired_at)  AS hour_start,
    severity,
    source,
    count()                  AS alert_count
FROM stream.alert_events
WHERE fired_at >= now() - INTERVAL 24 HOUR
GROUP BY hour_start, severity, source
ORDER BY hour_start DESC, severity;

-- Agent 操作成功率统计
CREATE VIEW IF NOT EXISTS stream.agent_success_rate AS
SELECT
    toDate(log_time)  AS log_date,
    skill_name,
    action_type,
    risk_level,
    count()           AS total,
    countIf(success=1 AND dry_run=0) AS executed_success,
    countIf(dry_run=1)               AS dry_run_count,
    countIf(allowed=0)               AS blocked_count
FROM stream.agent_decision_log
WHERE log_time >= now() - INTERVAL 7 DAY
GROUP BY log_date, skill_name, action_type, risk_level
ORDER BY log_date DESC, total DESC;

-- 高频告警 TopN（用于识别噪音）
CREATE VIEW IF NOT EXISTS stream.alert_top_noise AS
SELECT
    fingerprint,
    title,
    source,
    category,
    count()            AS fire_count,
    max(fired_at)      AS last_fired,
    min(severity)      AS worst_severity
FROM stream.alert_events
WHERE fired_at >= now() - INTERVAL 24 HOUR
GROUP BY fingerprint, title, source, category
ORDER BY fire_count DESC
LIMIT 20;

-- 未解决告警（有事件但无对应成功处置记录）
CREATE VIEW IF NOT EXISTS stream.alert_unresolved AS
SELECT
    ae.alert_id,
    ae.fired_at,
    ae.severity,
    ae.title,
    ae.source,
    ae.affected_tables,
    ae.downstream_tables
FROM stream.alert_events ae
LEFT JOIN (
    SELECT alert_id, max(success) AS resolved
    FROM stream.agent_decision_log
    WHERE dry_run = 0
    GROUP BY alert_id
) dl ON ae.alert_id = dl.alert_id
WHERE (dl.resolved = 0 OR dl.resolved IS NULL)
  AND ae.fired_at >= now() - INTERVAL 24 HOUR
ORDER BY ae.severity, ae.fired_at DESC;
