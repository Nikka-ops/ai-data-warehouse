-- ============================================================
-- AI ETL Agent 专用表
-- ============================================================

-- AI 生成并管理的清洗规则表
CREATE TABLE IF NOT EXISTS stream.etl_rules (
    rule_id       String,
    rule_name     String,
    rule_type     String,       -- fill_null / clamp_value / replace_invalid / custom
    target_table  String,       -- 作用的目标表（当前固定 dwd.realtime_order_detail）
    field_name    String,       -- 作用字段
    condition_sql String,       -- 触发条件（WHERE 子句片段）
    transform_expr String,      -- ClickHouse 转换表达式（用于 SELECT 中替换该字段）
    priority      UInt8  DEFAULT 50,  -- 执行优先级，数字越小越先执行
    enabled       UInt8  DEFAULT 1,   -- 1=启用 0=禁用
    generated_by  String DEFAULT 'ai',-- ai / manual
    ai_reason     String DEFAULT '',  -- AI 给出的规则理由
    hit_count     UInt64 DEFAULT 0,   -- 累计命中次数
    created_at    DateTime DEFAULT now(),
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY rule_id
SETTINGS index_granularity = 128;


-- ETL 每轮运行审计日志
CREATE TABLE IF NOT EXISTS stream.etl_audit_log (
    log_id           String,
    run_time         DateTime,
    window_start     DateTime,
    window_end       DateTime,
    records_scanned  UInt64,
    issues_found     UInt64,
    rules_applied    UInt64,
    records_fixed    UInt64,
    new_rules_count  UInt8,
    quality_score    Float32,   -- 数据质量分 0~100
    status           String,    -- success / partial / failed
    summary          String,
    _created_at      DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY run_time
TTL run_time + INTERVAL 30 DAY;


-- ============================================================
-- 多轮对话会话持久化表
-- ============================================================

CREATE TABLE IF NOT EXISTS stream.chat_sessions (
    session_id     String,           -- 会话唯一ID（UUID）
    session_name   String DEFAULT '', -- 用户自定义会话名（可选）
    turn_index     UInt32,            -- 轮次序号（0起）
    role           String,            -- user / assistant
    msg_type       String DEFAULT '', -- nl2sql / rag / text
    content        String DEFAULT '', -- 消息正文（问题或回答文本）
    sql_text       String DEFAULT '', -- NL2SQL 生成的 SQL
    result_summary String DEFAULT '', -- NL2SQL 结果摘要（供下轮历史注入）
    sources        String DEFAULT '', -- RAG 来源文件（逗号分隔）
    created_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (session_id, turn_index)
TTL created_at + INTERVAL 7 DAY;  -- 会话记录保留7天

