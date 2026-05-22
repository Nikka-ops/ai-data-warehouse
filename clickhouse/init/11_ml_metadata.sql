-- =============================================================================
-- 文件：11_ml_metadata.sql
-- 描述：ML 实验元数据中心（ML Metadata Store）
--
-- 架构理念（MLOps + Kappa）：
--   ML Metadata Store 是 MLOps 工程的神经中枢，负责：
--   1. 实验追踪（Experiment Tracking）：记录每次训练的超参数、指标和状态；
--   2. 模型-特征血缘（Model-Feature Lineage）：追踪模型依赖哪些特征及其重要性；
--   3. 预测日志（Prediction Log）：记录线上推理的输入特征和预测结果，
--      用于效果回归分析（Actual vs Predicted）和在线/离线一致性验证。
--
--   与 feature_store 的协作关系：
--   - experiments.dataset_id → feature_store.training_datasets.dataset_id
--   - model_feature_registry → feature_store.feature_definitions（特征依赖）
--   - prediction_log 的 features_json → 在线服务取自 feature_store（推理时特征快照）
--
-- 数据源：
--   ods.orders_stream   — 订单事件流
--   ods.payments_stream — 支付事件流
--
-- ClickHouse 版本：24.3+
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 创建 ML 元数据数据库
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ml_metadata;


-- =============================================================================
-- 表 1：ml_metadata.experiments
-- 用途：ML 实验追踪表（Experiment Tracking）
--
-- 设计思路：
--   每一次模型训练任务对应一条 experiments 记录。
--   这是 MLOps 可复现性的入口：
--   - experiment_name + started_at 共同唯一标识一次实验
--   - dataset_id 关联 feature_store.training_datasets，确保训练数据可溯源
--   - features_used 记录本次训练使用的特征列表（快照），防止特征删除后无法复现
--   - hyperparams：JSON 格式的超参数记录，如 {"n_estimators": 200, "max_depth": 6}
--   - metrics_json：JSON 格式的评估指标，如 {"auc": 0.85, "f1": 0.73, "rmse": 12.5}
--   - status 状态机：running → completed / failed
--   - finished_at 使用 Nullable(DateTime) 支持运行中的实验（尚未完成）
--
--   ReplacingMergeTree(updated_at) 允许更新实验状态和指标（通过重写相同 experiment_name + started_at）。
-- =============================================================================
CREATE TABLE IF NOT EXISTS ml_metadata.experiments
(
    -- 实验唯一 ID（UUID），供外部系统（如 MLflow、Kubeflow）引用
    experiment_id   String DEFAULT generateUUIDv4() COMMENT '实验唯一标识符，系统自动生成 UUID',
    -- 实验名称，通常对应业务场景（如 "customer_churn_v3", "gmv_forecast_2024Q1"）
    experiment_name String COMMENT '实验名称，对应业务预测任务，如 customer_churn_v3',
    -- 模型类型，如 "XGBoost", "LightGBM", "DeepFM", "LSTM"
    model_type      String COMMENT '模型算法类型，如 XGBoost / LightGBM / DeepFM / LSTM',
    -- 关联的训练数据集 ID（来自 feature_store.training_datasets）
    dataset_id      String COMMENT '训练数据集 ID，关联 feature_store.training_datasets',
    -- 本次实验实际使用的特征列表（快照，防止特征定义变更后无法复现）
    features_used   Array(String) COMMENT '本次训练使用的特征列表（快照），如 ["order_count_7d","gmv_7d"]',
    -- 超参数 JSON：记录完整的超参数配置
    -- 示例：{"n_estimators": 200, "max_depth": 6, "learning_rate": 0.05}
    hyperparams     String DEFAULT '{}' COMMENT '超参数 JSON，如 {"n_estimators":200,"max_depth":6}',
    -- 评估指标 JSON：记录所有评估指标
    -- 示例：{"auc": 0.856, "f1": 0.731, "precision": 0.782, "recall": 0.687}
    metrics_json    String DEFAULT '{}' COMMENT '评估指标 JSON，如 {"auc":0.856,"f1":0.731}',
    -- 状态机：running（训练中）→ completed（成功）/ failed（失败）
    status          LowCardinality(String) DEFAULT 'running'
        COMMENT '实验状态: running（训练中）/ completed（成功）/ failed（失败）',
    started_at      DateTime DEFAULT now() COMMENT '实验开始时间',
    -- Nullable 支持训练中的实验（finished_at = NULL 表示尚未完成）
    finished_at     Nullable(DateTime) COMMENT '实验完成时间，NULL 表示仍在运行中',
    updated_at      DateTime DEFAULT now() COMMENT '最后更新时间，ReplacingMergeTree 版本键'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (experiment_name, started_at)
COMMENT 'ML 实验追踪表：记录每次模型训练的超参数、评估指标和数据集，支持实验对比和复现';


-- =============================================================================
-- 表 2：ml_metadata.model_feature_registry
-- 用途：模型-特征重要性注册表（Model-Feature Registry）
--
-- 设计思路：
--   该表回答两个关键问题：
--   1. "这个模型依赖哪些特征？" → 当特征变更时，评估对哪些模型有影响
--   2. "每个特征对该模型的贡献有多大？" → 支持特征重要性分析和特征裁剪
--
--   importance_score：特征重要性分数（范围 0~1），来源可以是：
--   - 树模型的 feature_importance（split gain / cover）
--   - SHAP values（更可解释）
--   - 排列重要性（Permutation Importance）
--
--   与 feature_store.feature_lineage 的区别：
--   - lineage 追踪的是"数据流方向"（从哪里来到哪里去）
--   - model_feature_registry 追踪的是"模型对特征的依赖程度"（影响强弱）
--
--   ReplacingMergeTree 无版本键：按 ORDER BY 键去重，保留最后一次写入。
-- =============================================================================
CREATE TABLE IF NOT EXISTS ml_metadata.model_feature_registry
(
    -- 模型名称，对应业务预测任务（如 "churn_predictor", "price_estimator"）
    model_name          String COMMENT '模型名称，如 churn_predictor / price_estimator',
    -- 模型版本号，支持同一模型的多个版本并存（如 "v1", "v2.1", "20240315"）
    model_version       String COMMENT '模型版本号，如 v1 / v2.1 / 20240315',
    -- 特征所属的特征组（来自 feature_store.feature_groups）
    feature_group       String COMMENT '特征所属特征组，关联 feature_store.feature_groups',
    -- 特征名称（来自 feature_store.feature_definitions）
    feature_name        String COMMENT '特征名称，关联 feature_store.feature_definitions',
    -- 特征重要性分数（0~1），0表示无贡献，1表示最重要
    -- 通常所有特征的 importance_score 之和归一化为1
    importance_score    Float32 DEFAULT 0
        COMMENT '特征重要性分数（0~1），来源于 SHAP 值或树模型特征增益',
    registered_at       DateTime DEFAULT now() COMMENT '注册时间（模型训练完成后写入）'
)
ENGINE = ReplacingMergeTree()
ORDER BY (model_name, model_version, feature_group, feature_name)
COMMENT '模型-特征重要性注册表：记录每个模型版本依赖的特征及其重要性，支持特征影响分析';


-- =============================================================================
-- 表 3：ml_metadata.prediction_log
-- 用途：在线推理预测日志（Online Prediction Log）
--
-- 设计思路：
--   预测日志是连接"模型预测"与"业务结果"的桥梁，支持：
--   1. 在线/离线一致性验证（Online-Offline Consistency Check）：
--      对比 prediction_log 中记录的特征值与 feature_store.feature_values
--      中离线计算的值，发现 Training-Serving Skew（训练-服务偏差）
--   2. 效果回归分析（Actual vs Predicted）：
--      将预测结果与实际结果关联，计算模型在线精度
--   3. 公平性审计（Fairness Audit）：
--      分析不同群体（city/state）的预测分布，检测模型偏见
--
--   features_json：推理时使用的特征快照（JSON），格式如：
--   {"order_count_7d": 5, "gmv_7d": 258.50, "cancel_rate_30d": 0.02}
--
--   prediction_value：数值型预测结果（回归任务，如预测 GMV、物流时长）
--   prediction_label：类别型预测结果（分类任务，如 "high_risk"、"will_churn"）
--
--   TTL 30天：预测日志量巨大，仅保留近30天用于在线监控；
--   历史数据应定期归档到对象存储（S3/OSS）供离线分析。
--
--   分区策略：按月份分区，支持快速清理过期数据。
-- =============================================================================
CREATE TABLE IF NOT EXISTS ml_metadata.prediction_log
(
    -- 预测唯一 ID，可用于与业务系统的回调对账
    prediction_id       String DEFAULT generateUUIDv4() COMMENT '预测请求唯一 ID，用于与业务回调对账',
    -- 发起预测的模型名称
    model_name          String COMMENT '预测模型名称，关联 model_feature_registry',
    -- 发起预测的模型版本（支持灰度/A-B 测试期间多版本并行记录）
    model_version       String COMMENT '预测模型版本，支持 A/B 测试期间并行追踪',
    -- 被预测的实体 ID（如 customer_id 的具体值）
    entity_id           String COMMENT '被预测实体 ID，如 customer_id 具体值 "C001"',
    -- 推理发生的时间戳（精确到秒，用于分区和 TTL）
    prediction_time     DateTime DEFAULT now() COMMENT '推理请求时间戳，用于分区和 TTL 计算',
    -- 推理时使用的特征快照（JSON 序列化）
    -- 目的：保存推理现场，支持事后的在线/离线一致性验证
    features_json       String COMMENT '推理时特征快照（JSON），用于在线/离线一致性验证',
    -- 数值型预测值（回归任务使用，如预测订单金额、物流时长）
    prediction_value    Float64 COMMENT '数值型预测结果（回归任务），如预测 GMV = 158.50',
    -- 类别型预测标签（分类任务使用，如流失预测、风险分级）
    prediction_label    String DEFAULT '' COMMENT '类别型预测结果（分类任务），如 will_churn / high_risk'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(prediction_time)
ORDER BY prediction_time
-- 预测日志保留30天，超期自动删除（数据量大，仅保留用于在线监控的窗口）
TTL prediction_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192
COMMENT '在线推理预测日志：记录推理现场快照和预测结果，支持在线/离线一致性验证和效果回归分析';
