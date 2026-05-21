-- ============================================================
-- 自动告警处置系统：系统级告警 + 修复动作审计
-- ============================================================

-- ── 系统级告警表（区别于数据质量告警）────────────────────────
-- 来源：alert_investigator 定期探测 Kappa/ETL/Kafka 健康状态
CREATE TABLE IF NOT EXISTS stream.system_alerts (
    alert_id       String DEFAULT generateUUIDv4(),
    alert_time     DateTime DEFAULT now(),
    alert_type     LowCardinality(String),    -- KAPPA_REPLAY / KAFKA_LAG / ETL_QUALITY / FLINK_JOB
    severity       LowCardinality(String),    -- CRITICAL / HIGH / MEDIUM / LOW
    source         String,                    -- 告警来源组件
    title          String,                    -- 一行摘要
    detail         String,                    -- 详细描述
    metric_value   Float64 DEFAULT 0,         -- 触发告警的指标值
    threshold_value Float64 DEFAULT 0,        -- 告警阈值
    context_json   String DEFAULT '{}',       -- 采集到的上下文（JSON）
    handled        UInt8 DEFAULT 0            -- 0=待处理，1=已处理
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(alert_time)
ORDER BY (alert_time, severity)
TTL alert_time + INTERVAL 30 DAY
COMMENT '系统级告警（Kappa 回放 / Kafka Lag / ETL 质量 / Flink 作业）';

-- ── 自动处置动作审计表 ────────────────────────────────────────
-- 每次 auto-remediation 执行后写入，完整记录输入→分析→动作→结果
CREATE TABLE IF NOT EXISTS stream.remediation_actions (
    action_id        String DEFAULT generateUUIDv4(),
    alert_id         String,
    alert_type       LowCardinality(String),
    alert_severity   LowCardinality(String),
    action_time      DateTime DEFAULT now(),

    -- LLM 分析结果
    root_cause       String,
    impact_scope     String,
    confidence       Float32 DEFAULT 0,

    -- 执行的修复动作
    action_type      LowCardinality(String),  -- RESTART_REPLAY / TRIGGER_ETL / QUARANTINE / NOTIFY / NOOP
    action_detail    String,                  -- 动作参数描述
    action_result    String,                  -- 执行结果（成功/失败/跳过）
    action_success   UInt8 DEFAULT 0,         -- 1=成功，0=失败

    -- 最终状态
    final_status     LowCardinality(String),  -- resolved / monitoring / escalated / failed
    resolve_time     Nullable(DateTime),      -- 标记已解决的时间点
    feedback_sent    UInt8 DEFAULT 0,         -- 是否已发送反馈通知

    raw_context      String DEFAULT '{}'
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(action_time)
ORDER BY action_time
TTL action_time + INTERVAL 90 DAY
COMMENT '告警自动处置动作审计（完整处置链路记录）';

-- ── 统一告警视图（数据质量 + 系统告警合并）───────────────────
CREATE VIEW IF NOT EXISTS stream.alert_unified AS
SELECT
    alert_id,
    alert_time,
    alert_type,
    severity,
    'data_quality' AS category,
    detail         AS title,
    detail,
    metric_value,
    threshold_value
FROM stream.ai_quality_alerts
UNION ALL
SELECT
    alert_id,
    alert_time,
    alert_type,
    severity,
    'system'  AS category,
    title,
    detail,
    metric_value,
    threshold_value
FROM stream.system_alerts;

-- ── 告警处置状态看板视图 ──────────────────────────────────────
CREATE VIEW IF NOT EXISTS stream.remediation_dashboard AS
SELECT
    action_time,
    alert_type,
    alert_severity,
    action_type,
    root_cause,
    action_detail,
    action_result,
    action_success,
    final_status,
    round(confidence, 2) AS confidence
FROM stream.remediation_actions
ORDER BY action_time DESC
LIMIT 50;
