-- ============================================================
-- 告警自动排查记录表
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.alert_investigations (
    investigation_id String DEFAULT generateUUIDv4(),
    alert_id         String,
    alert_type       String,
    alert_severity   String,
    investigation_time DateTime DEFAULT now(),
    root_cause       String,    -- LLM 分析的根本原因
    impact_scope     String,    -- 影响范围描述
    auto_action      String,    -- 已自动执行的操作
    action_result    String,    -- 操作结果
    confidence       Float32,   -- LLM 置信度（0-1）
    status           String,    -- 'resolved' / 'monitoring' / 'escalated'
    raw_context      String     -- 排查时的原始数据上下文（JSON）
) ENGINE = MergeTree()
ORDER BY investigation_time
TTL investigation_time + INTERVAL 30 DAY
COMMENT 'AI 告警自动排查与处置记录';
