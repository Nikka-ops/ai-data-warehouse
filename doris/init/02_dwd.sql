-- ============================================================
-- 02 DWD 层：订单累积快照
--
-- 这张表承载的是订单的「当前状态」，不是事件。
-- 数据源是 MySQL order_info 的 binlog changelog：同一个 order_id 会随着
-- CREATED → PAID → SHIPPED → DELIVERED 被 Flink 写入很多次。
--
-- 模型选型：UNIQUE KEY + Merge-on-Write + Sequence Column，三件事各解决一个问题。
--
-- 1) Unique Key 保证幂等
--    Flink 作业从 checkpoint 恢复时会重放一段数据，同一条订单变更可能被
--    写入多次。Unique Key 按主键覆盖，重放多少次结果都一样 —— 这是端到端
--    Exactly-Once 里「Sink 端幂等」的那一环，和 Flink 的两阶段提交配合，
--    才凑成完整的一致性保证。
--
-- 2) Merge-on-Write 让点查变快
--    Doris 2.x 的 Unique 表默认 MoW，写入时就完成去重，查询不再需要
--    运行时 merge。看板按 order_id 点查、按状态过滤时差别很明显。
--
-- 3) Sequence Column 解决乱序覆盖
--    binlog 事件经过 Kafka 多分区、Flink 多并行度之后，到达顺序不再保证。
--    没有 sequence column 时，「后写入的赢」—— 一条迟到的 CREATED 事件
--    能把已经是 DELIVERED 的订单改回 CREATED。
--    指定 update_time 为 sequence column 后变成「事件时间大的赢」，
--    旧状态永远盖不掉新状态。这是"同一份数据无论被处理几次都收敛到
--    同一个正确值"的关键。
--
-- 分区分桶：
--    按 create_date 天分区，动态分区自动滚动，看板永远只扫当天那个分区；
--    按 order_id 哈希分 6 桶，与 BE 的 CPU 核数匹配，单分区内可并行扫描。
-- ============================================================

CREATE TABLE IF NOT EXISTS dwd.order_snapshot (
    -- ── Key（Unique Key 必须是表定义里最靠前的列）──
    create_date         DATE            NOT NULL              COMMENT '下单日期，分区列',
    order_id            BIGINT          NOT NULL              COMMENT '订单ID',

    -- ── Sequence 列：事件时间大的版本获胜，杜绝迟到的旧状态覆盖新状态 ──
    update_time         DATETIME(3)     NULL                  COMMENT '业务库更新时间（sequence column）',

    -- ── 维度（已在 Flink 侧退化，查询不必再 join）──
    user_id             BIGINT          NULL                  COMMENT '用户ID',
    province_id         INT             NULL                  COMMENT '省份ID',
    province_name       VARCHAR(64)     NULL                  COMMENT '省份名',
    region_name         VARCHAR(64)     NULL                  COMMENT '大区名',
    order_status        VARCHAR(32)     NULL                  COMMENT 'CREATED/PAID/SHIPPED/DELIVERED/CANCELED/REFUNDED',

    -- ── 金额 ──
    total_amount        DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '订单总额',
    activity_reduce     DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '活动优惠',
    coupon_reduce       DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '优惠券优惠',
    freight_amount      DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '运费',

    -- ── 状态流转时间戳：累积快照相比事务事实表的价值就在这几列 ──
    create_time         DATETIME(3)     NULL                  COMMENT '下单时间',
    payment_time        DATETIME(3)     NULL                  COMMENT '支付时间',
    ship_time           DATETIME(3)     NULL                  COMMENT '发货时间',
    receive_time        DATETIME(3)     NULL                  COMMENT '收货时间',

    -- 下单到支付的时长。跨状态的时间差只有在快照表里才算得出来，
    -- 事务事实表每行只有一个时间点，做不到。
    pay_lag_seconds     INT             NULL                  COMMENT '下单到支付耗时（秒），未支付为 NULL',

    ingest_time         DATETIME(3)     NULL                  COMMENT 'Flink 落库时间'
) ENGINE = OLAP
UNIQUE KEY(create_date, order_id)
COMMENT '订单累积快照 - DWD层（Flink CDC 直写，主键幂等 + sequence 防乱序）'
PARTITION BY RANGE(create_date) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 6
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "update_time",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-15",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "6",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "15"
);
