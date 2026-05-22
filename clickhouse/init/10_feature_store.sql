-- =============================================================================
-- 文件：10_feature_store.sql
-- 描述：AI 数据仓库特征存储（Feature Store）核心 Schema
--
-- 架构理念（Kappa + Feature Store）：
--   在 Kappa 架构下，所有特征均来源于同一条实时数据流（Kafka → Flink → ClickHouse）。
--   Feature Store 作为机器学习的"数据超市"，负责：
--     1. 统一管理特征定义、计算逻辑和 SLA 契约；
--     2. 分离在线特征（低延迟查询）与离线特征（批量训练集生成）；
--     3. 追踪特征血缘（Lineage），确保训练/推理一致性，避免 Training-Serving Skew；
--     4. 监控特征漂移（Feature Drift），及时发现数据分布变化。
--
-- 数据源：
--   ods.orders_stream   — 订单事件流（order_id, customer_id, product_category, ...)
--   ods.payments_stream — 支付事件流（payment_id, order_id, payment_type, ...)
--
-- ClickHouse 版本：24.3+
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 创建特征存储数据库
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS feature_store;


-- =============================================================================
-- 表 1：feature_store.feature_groups
-- 用途：特征组元数据注册表
--
-- 设计思路：
--   特征组（Feature Group）是特征的逻辑容器，对应一个业务实体（entity）。
--   例如："user_behavior" 组对应 customer_id 实体，
--         "category_stats" 组对应 product_category 实体。
--   使用 ReplacingMergeTree 保证按 group_name 去重（最新版本保留），
--   适合低频的元数据写入场景。
-- =============================================================================
CREATE TABLE IF NOT EXISTS feature_store.feature_groups
(
    -- 特征组唯一名称，作为主键（ORDER BY 键）
    group_name   String COMMENT '特征组名称，全局唯一，如 user_behavior / category_stats',
    -- 该特征组绑定的实体主键字段名（用于 point-in-time 查询）
    entity_key   String COMMENT '实体主键字段，如 customer_id / product_category',
    description  String COMMENT '业务描述，说明该特征组的业务含义和使用场景',
    owner        String COMMENT '负责团队或负责人，用于告警通知和权限管理',
    created_at   DateTime DEFAULT now() COMMENT '首次注册时间',
    updated_at   DateTime DEFAULT now() COMMENT '最后更新时间，用于 ReplacingMergeTree 版本选择'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY group_name
COMMENT '特征组元数据注册表：管理特征的业务实体映射关系';


-- =============================================================================
-- 表 2：feature_store.feature_definitions
-- 用途：单个特征的详细元数据（特征目录）
--
-- 设计思路：
--   每条记录描述一个特征的完整生命周期信息：
--   - computation_sql：特征计算逻辑，可直接在 ClickHouse 执行
--   - refresh_schedule：刷新频率（cron 表达式）
--   - online_ttl：在线存储的 TTL（秒），控制 Redis/低延迟存储的过期时间
--   - max_staleness_seconds：最大允许陈旧时间，超过则触发告警或降级
--   - version：支持特征多版本管理，新模型可引用旧版特征
--
--   ReplacingMergeTree(updated_at) 确保同名特征只保留最新定义。
-- =============================================================================
CREATE TABLE IF NOT EXISTS feature_store.feature_definitions
(
    -- 特征唯一 ID（UUID），方便外部系统引用
    feature_id              String DEFAULT generateUUIDv4() COMMENT '特征唯一标识符，系统自动生成 UUID',
    -- 所属特征组，与 feature_groups.group_name 关联
    group_name              String COMMENT '所属特征组名称，关联 feature_groups',
    -- 特征名称，在同一特征组内唯一
    feature_name            String COMMENT '特征名称，同一特征组内唯一，如 order_count_7d',
    -- 特征数据类型枚举
    -- INT64   : 整数特征（计数、分类编码等）
    -- FLOAT64 : 浮点特征（金额、比例、统计量等）
    -- STRING  : 字符串特征（类别特征原始值等）
    -- BOOLEAN : 布尔特征（是否标志位等）
    -- VECTOR  : 向量特征（Embedding 等，存储为序列化字符串）
    feature_type            LowCardinality(String) COMMENT '特征类型: INT64 / FLOAT64 / STRING / BOOLEAN / VECTOR',
    description             String COMMENT '特征业务描述，说明计算逻辑和业务含义',
    -- 特征计算 SQL：可直接在 ClickHouse 执行的查询，
    -- 返回格式为 (entity_id, feature_value, feature_time)
    computation_sql         String COMMENT '特征计算 SQL，返回 (entity_id, feature_value, feature_time)',
    -- Cron 表达式，如 "*/5 * * * *" 表示每5分钟刷新
    refresh_schedule        String COMMENT '特征刷新调度频率，cron 表达式或 on_demand',
    -- 在线特征存储（Redis 等）的 TTL（秒）
    online_ttl              UInt32 COMMENT '在线存储 TTL（秒），控制低延迟缓存层的过期',
    -- 当特征计算失败或缺失时使用的默认值（字符串序列化）
    default_value           String COMMENT '特征缺失时的默认值（字符串序列化，如 "0" / "0.0"）',
    -- 超过该时间未刷新则视为"陈旧"特征，触发 SLA 违规
    max_staleness_seconds   UInt32 COMMENT '最大允许陈旧时间（秒），超过则降级使用默认值',
    -- 特征版本号，支持特征的灰度发布和 A/B 测试
    version                 UInt32 DEFAULT 1 COMMENT '特征版本号，支持多版本并存',
    -- 软删除标志，is_active=0 时特征停止计算但保留历史数据
    is_active               UInt8 DEFAULT 1 COMMENT '是否启用：1=启用，0=停用（软删除）',
    -- 标签数组，用于特征分类检索（如 ["behavioral", "recency"]）
    tags                    Array(String) COMMENT '特征标签数组，用于分类和检索，如 ["behavioral","recency"]',
    created_at              DateTime DEFAULT now() COMMENT '特征首次注册时间',
    updated_at              DateTime DEFAULT now() COMMENT '最后更新时间，ReplacingMergeTree 版本键'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (group_name, feature_name)
COMMENT '特征定义目录：记录每个特征的计算逻辑、类型和 SLA 元数据';


-- =============================================================================
-- 表 3：feature_store.feature_values
-- 用途：离线特征值存储（Offline Feature Store）
--
-- 设计思路：
--   离线特征存储用于：
--   1. 生成训练样本集（Point-in-Time 正确性：使用 feature_time 而非 now() 关联标签）；
--   2. 批量回刷历史特征；
--   3. 特征质量监控的数据来源。
--
--   分区策略：按 (月份, group_name) 双维度分区
--   - 按月分区降低单分区数据量，加速时间范围扫描；
--   - 按 group_name 分区使不同特征组的查询相互隔离，避免热点。
--
--   TTL 90天：离线特征历史保留3个月，超期自动清理，控制存储成本。
--
--   排序键设计：(entity_id, group_name, feature_name, feature_time)
--   - 先按实体ID排序，支持单实体全特征历史查询；
--   - 最后按时间排序，支持 asof-join（Point-in-Time 查询）。
-- =============================================================================
CREATE TABLE IF NOT EXISTS feature_store.feature_values
(
    -- 实体 ID，对应 feature_groups.entity_key 字段的值
    -- 如 customer_id="C001", product_category="electronics"
    entity_id           String COMMENT '实体 ID 值，如 customer_id 的具体值 "C001"',
    group_name          String COMMENT '所属特征组名称',
    feature_name        String COMMENT '特征名称',
    -- 数值型特征值（INT64/FLOAT64 特征存储于此）
    feature_value       Float64 COMMENT '数值型特征值（INT64/FLOAT64 类型）',
    -- 字符串型特征值（STRING/BOOLEAN/VECTOR 特征存储于此）
    feature_value_str   String DEFAULT '' COMMENT '字符串型特征值（STRING/VECTOR 类型）',
    -- 特征的业务时间戳（非写入时间），用于 Point-in-Time 正确性
    feature_time        DateTime COMMENT '特征业务时间戳，Point-in-Time 查询的关联键',
    computed_at         DateTime DEFAULT now() COMMENT '特征实际计算完成时间，用于 ReplacingMergeTree 去重',
    -- 对应 feature_definitions.version，追踪使用哪个版本的计算逻辑生成
    version             UInt32 DEFAULT 1 COMMENT '计算该特征值时使用的特征定义版本号'
)
ENGINE = ReplacingMergeTree(computed_at)
-- 双维度分区：月份 + 特征组，兼顾时间范围查询和特征组隔离
PARTITION BY (toYYYYMM(feature_time), group_name)
ORDER BY (entity_id, group_name, feature_name, feature_time)
-- 离线特征保留90天，超期自动删除（TTL 基于业务时间 feature_time）
TTL feature_time + INTERVAL 90 DAY
SETTINGS index_granularity = 8192
COMMENT '离线特征值存储：支持 Point-in-Time 查询和训练集生成，TTL 90天自动清理';


-- =============================================================================
-- 表 4：feature_store.feature_contracts
-- 用途：特征 SLA 契约（Feature Contract）
--
-- 设计思路：
--   Feature Contract 是 Feature Store 与下游模型/服务之间的"接口协议"，定义：
--   - 数据新鲜度 SLA（sla_freshness_seconds）：超时需告警
--   - 覆盖率下界（min_coverage_pct）：低于此比例触发数据质量告警
--   - 最大陈旧容忍（max_staleness_seconds）：超过则执行 on_breach_action
--   - 违约行为（on_breach_action）：
--       "use_default"  — 使用默认值，保障推理服务可用性
--       "raise_error"  — 直接报错，适合高精度要求场景
--       "skip_feature" — 跳过该特征，由模型处理缺失值
--
--   与 feature_definitions 分离的原因：
--   同一特征在不同服务中 SLA 要求可能不同（如实时推荐 vs 离线报表），
--   contract 可按调用方单独配置。
-- =============================================================================
CREATE TABLE IF NOT EXISTS feature_store.feature_contracts
(
    group_name              String COMMENT '所属特征组名称',
    feature_name            String COMMENT '特征名称',
    -- 当特征不可用时，数值型特征的降级默认值
    default_value_float     Float64 DEFAULT 0 COMMENT '数值型特征降级默认值',
    -- 当特征不可用时，字符串型特征的降级默认值
    default_value_str       String DEFAULT '' COMMENT '字符串型特征降级默认值',
    -- 超过此秒数未更新则视为"陈旧"，触发 on_breach_action
    max_staleness_seconds   UInt32 DEFAULT 3600 COMMENT '最大陈旧容忍（秒），超过则执行违约动作',
    -- 特征覆盖率下界（0.9 = 至少90%的实体有有效特征值）
    min_coverage_pct        Float32 DEFAULT 0.9 COMMENT '最低特征覆盖率（0~1），低于此值触发质量告警',
    -- SLA 新鲜度要求（秒）：特征计算延迟必须低于此值
    sla_freshness_seconds   UInt32 DEFAULT 300 COMMENT 'SLA 新鲜度要求（秒），特征延迟超过此值告警',
    -- 违约时的处理动作：use_default / raise_error / skip_feature
    on_breach_action        LowCardinality(String) DEFAULT 'use_default'
        COMMENT '违约处理动作: use_default（降级）/ raise_error（报错）/ skip_feature（跳过）',
    updated_at              DateTime DEFAULT now() COMMENT '契约最后更新时间，ReplacingMergeTree 版本键'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (group_name, feature_name)
COMMENT '特征 SLA 契约：定义特征的新鲜度要求、覆盖率下界和违约处理策略';


-- =============================================================================
-- 表 5：feature_store.feature_lineage
-- 用途：特征血缘追踪（Feature Lineage）
--
-- 设计思路：
--   特征血缘是 MLOps 可解释性和可审计性的基础。它回答：
--   "这个特征是从哪里来的？依赖哪张表？经过了什么转换？"
--
--   血缘记录分为两类节点：
--   - source_type: kafka_topic（原始流）/ clickhouse_table（中间表）/ feature_group（上游特征）
--   - target_type: 同上
--
--   例如：
--   kafka_topic:ods.orders_stream → feature_group:user_behavior（transformation_sql=聚合 SQL）
--   feature_group:user_behavior   → clickhouse_table:ml_metadata.training_datasets
--
--   TTL 180天：血缘信息保留半年，支持历史模型审计。
-- =============================================================================
CREATE TABLE IF NOT EXISTS feature_store.feature_lineage
(
    lineage_id          String DEFAULT generateUUIDv4() COMMENT '血缘记录唯一 ID',
    -- 上游节点类型：kafka_topic / clickhouse_table / feature_group
    source_type         LowCardinality(String)
        COMMENT '上游节点类型: kafka_topic / clickhouse_table / feature_group',
    -- 上游节点名称，如 "ods.orders_stream" 或 "user_behavior"
    source_name         String COMMENT '上游节点名称，如 ods.orders_stream',
    -- 下游节点类型
    target_type         LowCardinality(String)
        COMMENT '下游节点类型: kafka_topic / clickhouse_table / feature_group',
    -- 下游节点名称，如 "user_behavior" 或 "ml_metadata.training_datasets"
    target_name         String COMMENT '下游节点名称，如 user_behavior',
    -- 从上游到下游的转换 SQL（可为空，表示直接透传）
    transformation_sql  String DEFAULT '' COMMENT '数据转换 SQL，空表示直接透传',
    recorded_at         DateTime DEFAULT now() COMMENT '血缘关系记录时间'
)
ENGINE = MergeTree()
ORDER BY recorded_at
-- 血缘信息保留180天，支持半年内的模型审计和溯源
TTL recorded_at + INTERVAL 180 DAY
SETTINGS index_granularity = 8192
COMMENT '特征血缘追踪：记录特征从原始数据源到最终使用的完整数据流向，支持可解释性审计';


-- =============================================================================
-- 表 6：feature_store.drift_stats
-- 用途：特征漂移统计（Feature Drift Monitoring）
--
-- 设计思路：
--   特征漂移（Feature Drift）是模型性能退化的主要原因之一。
--   当生产环境的特征分布与训练时显著不同，模型预测质量会下降。
--
--   关键指标：
--   - mean_value / std_value：均值和标准差，监控数值分布的中心和离散程度
--   - p50 / p95：中位数和95分位数，对异常值不敏感的分布指标
--   - null_rate：空值/零值比例，监控数据质量
--   - psi_score：Population Stability Index（群体稳定性指数）
--       PSI < 0.1：分布稳定，无需干预
--       0.1 ≤ PSI < 0.2：轻微漂移，关注监控
--       PSI ≥ 0.2：显著漂移，需要模型重训练
--   - drift_detected：综合判断标志，1=检测到漂移，触发告警
--
--   TTL 30天：漂移统计保留30天（通常只需最近几期对比），节省存储。
-- =============================================================================
CREATE TABLE IF NOT EXISTS feature_store.drift_stats
(
    feature_name    String COMMENT '特征名称',
    group_name      String COMMENT '所属特征组名称',
    check_time      DateTime DEFAULT now() COMMENT '漂移检测执行时间',
    -- 当前周期特征值的均值
    mean_value      Float64 COMMENT '当前统计周期内特征均值',
    -- 当前周期特征值的标准差
    std_value       Float64 COMMENT '当前统计周期内特征标准差',
    -- 50分位数（中位数）
    p50             Float64 COMMENT '特征值中位数（P50）',
    -- 95分位数（高分位数异常监控）
    p95             Float64 COMMENT '特征值95分位数（P95），监控高值异常',
    -- 空值率（NULL 或 0 值占总记录比例）
    null_rate       Float64 COMMENT '空值率（NULL/零值占比），监控数据完整性',
    -- PSI 分数：与基准分布（训练时分布）的偏差
    psi_score       Float64 DEFAULT 0 COMMENT 'PSI 分数（群体稳定性指数）：<0.1正常，≥0.2需重训练',
    -- 综合漂移检测结果：1=检测到显著漂移，触发模型重训练告警
    drift_detected  UInt8 DEFAULT 0 COMMENT '漂移标志：1=检测到显著漂移，需要触发模型重训练'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(check_time)
ORDER BY (group_name, feature_name, check_time)
-- 漂移统计保留30天，节省存储（通常只需最近几个统计周期做对比）
TTL check_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192
COMMENT '特征漂移监控统计：记录特征分布统计量和 PSI 分数，支持自动化模型重训练触发';


-- =============================================================================
-- 表 7：feature_store.training_datasets
-- 用途：训练数据集注册表（Training Dataset Registry）
--
-- 设计思路：
--   训练集注册表是 MLOps 可复现性（Reproducibility）的核心：
--   - 记录每个训练集使用了哪些特征组（feature_groups）
--   - 记录标签来源（label_table, label_column）
--   - 记录时间范围（start_time, end_time），支持 Point-in-Time 特征关联
--   - 记录生成状态（status）：pending → generating → ready → failed
--   - 记录文件路径（file_path）：可以是 HDFS/S3/本地路径
--
--   任何模型实验（ml_metadata.experiments）都应关联一个 dataset_id，
--   确保实验结果可追溯、可复现。
-- =============================================================================
CREATE TABLE IF NOT EXISTS feature_store.training_datasets
(
    -- 数据集唯一 ID，关联 ml_metadata.experiments.dataset_id
    dataset_id      String DEFAULT generateUUIDv4() COMMENT '训练集唯一 ID，关联 ml_metadata.experiments',
    dataset_name    String COMMENT '训练集名称，如 "churn_prediction_v2"',
    description     String COMMENT '训练集描述，记录业务场景和特征工程说明',
    -- 使用的特征组列表，如 ["user_behavior", "category_stats"]
    feature_groups  Array(String) COMMENT '包含的特征组列表，如 ["user_behavior","category_stats"]',
    -- 标签来源表（如 dws.customer_churn_labels）
    label_table     String COMMENT '标签来源表名（全限定名，如 dws.customer_churn_labels）',
    -- 标签字段名（如 "is_churned"）
    label_column    String COMMENT '标签字段名，如 is_churned / purchase_amount',
    -- 训练样本的时间范围起点（Point-in-Time 切割基准）
    start_time      DateTime COMMENT '样本时间范围起始时间（Point-in-Time 左边界）',
    -- 训练样本的时间范围终点
    end_time        DateTime COMMENT '样本时间范围结束时间（Point-in-Time 右边界）',
    -- 实际生成的样本行数（生成完成后更新）
    row_count       UInt64 DEFAULT 0 COMMENT '训练集总行数（生成完成后回填）',
    -- 数据集存储路径（支持 HDFS/S3/本地文件系统）
    file_path       String DEFAULT '' COMMENT '数据集存储路径，如 s3://bucket/datasets/churn_v2.parquet',
    -- 生成状态：pending（待生成）→ generating（生成中）→ ready（就绪）→ failed（失败）
    status          LowCardinality(String) DEFAULT 'pending'
        COMMENT '数据集状态: pending / generating / ready / failed',
    created_by      String DEFAULT 'system' COMMENT '创建者（用户名或系统服务名）',
    created_at      DateTime DEFAULT now() COMMENT '数据集注册时间，ReplacingMergeTree 版本键'
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (dataset_name, created_at)
COMMENT '训练数据集注册表：保障 MLOps 可复现性，追踪每个训练集的特征组成和标签来源';


-- =============================================================================
-- 视图 1：feature_store.feature_freshness
-- 用途：特征新鲜度监控视图
--
-- 设计思路：
--   该视图实时计算每个特征的"陈旧程度"：
--   - last_feature_time：该特征最新一条记录的业务时间
--   - staleness_seconds：当前时间与最新特征时间的差值（秒）
--   - is_stale：是否超过 max_staleness_seconds 阈值
--       1 = 特征已陈旧，需要触发重新计算或告警
--       0 = 特征新鲜，满足 SLA 要求
--
--   运维场景：
--   SELECT * FROM feature_store.feature_freshness WHERE is_stale = 1;
--   → 快速定位所有当前陈旧的特征，驱动自动化重刷任务。
-- =============================================================================
CREATE OR REPLACE VIEW feature_store.feature_freshness AS
SELECT
    fv.group_name,
    fv.feature_name,
    -- 最新特征的业务时间戳
    max(fv.feature_time)                                AS last_feature_time,
    -- 当前时间与最新特征时间的差值（秒）
    toInt64(now() - max(fv.feature_time))               AS staleness_seconds,
    -- 对应特征组契约中的最大陈旧容忍（取契约表中唯一值）
    max(fc.max_staleness_seconds)                       AS max_staleness_seconds,
    -- 是否已陈旧：超过契约定义的 max_staleness_seconds 则为1
    if(
        toInt64(now() - max(fv.feature_time)) > max(fc.max_staleness_seconds),
        1,
        0
    )                                                   AS is_stale
FROM feature_store.feature_values AS fv
-- 左连接契约表，获取该特征的 SLA 配置
LEFT JOIN feature_store.feature_contracts AS fc
    ON fv.group_name = fc.group_name
    AND fv.feature_name = fc.feature_name
GROUP BY
    fv.group_name,
    fv.feature_name;


-- =============================================================================
-- 视图 2：feature_store.feature_coverage
-- 用途：特征覆盖率监控视图
--
-- 设计思路：
--   特征覆盖率（Feature Coverage）衡量"有多少实体拥有有效的特征值"。
--   低覆盖率意味着大量实体在训练/推理时只能使用默认值，影响模型精度。
--
--   指标说明：
--   - total_entities：该特征共有多少不同的实体 ID
--   - null_or_zero_count：feature_value=0 且 feature_value_str='' 的记录数
--       （近似代表"无效值"，实际可按业务调整判断逻辑）
--   - null_rate：空/零值比率（0~1），值越低覆盖率越好
--   - coverage_rate：有效特征覆盖率 = 1 - null_rate
--
--   运维场景：
--   SELECT * FROM feature_store.feature_coverage WHERE coverage_rate < 0.9;
--   → 发现覆盖率低于90%的特征，对照 feature_contracts.min_coverage_pct 触发告警。
-- =============================================================================
CREATE OR REPLACE VIEW feature_store.feature_coverage AS
SELECT
    group_name,
    feature_name,
    -- 拥有该特征的不同实体总数
    count(DISTINCT entity_id)                                   AS total_entities,
    -- 总记录数（含重复实体的历史记录）
    count()                                                     AS total_records,
    -- 空值/零值记录数（feature_value=0 且 feature_value_str 为空则认为无效）
    countIf(feature_value = 0 AND feature_value_str = '')       AS null_or_zero_count,
    -- 空/零值比率
    round(
        countIf(feature_value = 0 AND feature_value_str = '') / count(),
        4
    )                                                           AS null_rate,
    -- 有效特征覆盖率（1 - null_rate）
    round(
        1.0 - countIf(feature_value = 0 AND feature_value_str = '') / count(),
        4
    )                                                           AS coverage_rate,
    -- 最后一次特征计算时间（数据集新鲜度参考）
    max(computed_at)                                            AS last_computed_at
FROM feature_store.feature_values
GROUP BY
    group_name,
    feature_name;
