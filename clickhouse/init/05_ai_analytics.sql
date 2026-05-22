-- ============================================================
-- AI 分析增强层：预测表 + 主动洞察表
-- ============================================================

-- 实时预测表（Holt双指数平滑，每分钟更新，预测未来10分钟）
CREATE TABLE IF NOT EXISTS dws.realtime_forecast (
    forecast_time DateTime,           -- 预测的目标时间点
    metric        String,             -- order_cnt / total_gmv / avg_price
    predicted     Float64,            -- 预测值
    lower_bound   Float64,            -- 95% 置信下界
    upper_bound   Float64,            -- 95% 置信上界
    horizon       UInt8,              -- 距当前的分钟数（1~10）
    model         String DEFAULT 'holt_double',
    _created_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_created_at)
ORDER BY (forecast_time, metric)
TTL forecast_time + INTERVAL 2 HOUR;


-- AI 主动洞察表（洞察引擎每5分钟生成）
CREATE TABLE IF NOT EXISTS stream.proactive_insights (
    insight_id   String,
    generated_at DateTime,
    period_start DateTime,
    period_end   DateTime,
    insight_type String,   -- trend_up / trend_down / anomaly / summary / opportunity
    title        String,   -- 一句话标题（≤40字）
    content      String,   -- 完整洞察（Markdown，含数字支撑）
    data_context String,   -- 生成时的原始数据摘要（JSON，供审计）
    priority     UInt8 DEFAULT 50,
    _created_at  DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY generated_at
TTL generated_at + INTERVAL 24 HOUR;


-- NL2DDL 创建的视图注册表（记录 AI 创建的自定义视图）
CREATE TABLE IF NOT EXISTS stream.custom_views (
    view_id      String,
    view_name    String,        -- 完整视图名，如 ads.seller_hourly
    description  String,        -- 用户原始描述
    ddl_sql      String,        -- 执行的 CREATE VIEW SQL
    created_by   String DEFAULT 'nl2ddl',
    created_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY created_at;
