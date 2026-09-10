-- ============================================================
-- 03 DWS 层
--
-- 这一层混用 Unique 和 Aggregate 两种模型，分界线是「谁来做聚合」：
--
--   Flink 已经算完整窗口   → Unique Key（REPLACE 语义）
--   Doris 自己把多行滚起来 → Aggregate Key（SUM / BITMAP_UNION 语义）
--
-- 这个分界很容易踩坑，值得展开：
-- 窗口聚合表如果建成 Aggregate + SUM，作业从 checkpoint 重启重放时，
-- 同一个窗口的结果会被再加一遍，GMV 直接翻倍 —— Aggregate + SUM 不是幂等的。
-- 而 Flink 的窗口算子对每个 (窗口, 维度) 只吐一行完整结果，
-- 用 Unique Key 按主键覆盖，重放几次都是同一个值。
-- 所以凡是 Flink 直写的表，一律 Unique。
--
-- 反过来，日活 BITMAP 表是 Doris 自己从 DWD 明细 INSERT SELECT 上来的，
-- 天然需要把多行滚成一行，Aggregate + BITMAP_UNION 才是对的。
-- ============================================================


-- ============================================================
-- 交易域窗口聚合（Flink 写入）
--
-- 一张表同时装两种窗口，用 window_type 区分：
--   TUMBLE_1M   —— 1 分钟滚动窗口，看瞬时波动、给质检器做基线
--   CUMULATE_1D —— 当日累计窗口，看板上「今日累计 GMV」要的就是它
--
-- CUMULATE 是增量维护的：窗口起点钉在零点，终点每分钟前推一格，
-- 计算代价与当天已积累的数据量无关，不像每次全表 SUM 那样越跑越慢。
--
-- category1_name / region_name 取值 'ALL' 表示汇总粒度。
-- Flink 侧用 GROUPING SETS 一次算出「整体 / 分品类 / 分大区」三个粒度，
-- 非分组维度会是 NULL，而 Doris 的 Key 列不允许为空，所以统一 COALESCE 成 'ALL'。
-- 代价是查询侧必须把两个维度列同时约束住 —— 只约束一个，另一个维度的
-- 汇总行会和明细行叠加，指标翻倍且不报错。
-- ============================================================
CREATE TABLE IF NOT EXISTS dws.trade_window_agg (
    -- ── Unique Key：一个窗口 + 一组维度唯一确定一行 ──
    stat_date           DATE            NOT NULL              COMMENT '统计日期，分区列',
    window_type         VARCHAR(16)     NOT NULL              COMMENT 'TUMBLE_1M / CUMULATE_1D',
    window_start        DATETIME(3)     NOT NULL              COMMENT '窗口起点',
    category1_name      VARCHAR(64)     NOT NULL              COMMENT '一级品类，ALL 表示不分品类',
    region_name         VARCHAR(64)     NOT NULL              COMMENT '大区，ALL 表示不分大区',

    -- ── 业务指标 ──
    window_end          DATETIME(3)     NULL                  COMMENT '窗口终点',
    order_cnt           BIGINT          NULL DEFAULT "0"      COMMENT '订单数（窗口内去重）',
    order_user_cnt      BIGINT          NULL DEFAULT "0"      COMMENT '下单用户数（Flink 状态内精确去重）',
    sku_num             BIGINT          NULL DEFAULT "0"      COMMENT '商品件数',
    order_amount        DECIMAL(18, 2)  NULL DEFAULT "0"      COMMENT 'GMV（分摊后成交额）',
    max_order_price     DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '窗口内最高单价',
    abnormal_price_cnt  BIGINT          NULL DEFAULT "0"      COMMENT '成交价显著偏离标价的条数',

    -- ── 链路时效观测 ──
    -- lag = 事件在业务库发生 → Flink 加工完成 的秒数，
    -- 这是「实时到底有多实时」唯一能拿出来的证据。
    max_lag_seconds     INT             NULL DEFAULT "0"      COMMENT '窗口内最大端到端延迟（秒）',
    avg_lag_seconds     INT             NULL DEFAULT "0"      COMMENT '窗口内平均端到端延迟（秒）',

    ingest_time         DATETIME(3)     NULL                  COMMENT 'Flink 落库时间'
) ENGINE = OLAP
UNIQUE KEY(stat_date, window_type, window_start, category1_name, region_name)
COMMENT '交易域窗口聚合 - DWS层（Flink 写入，主键幂等）'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 6
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-15",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "6",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "15"
);


-- ============================================================
-- 支付域窗口聚合（Flink 写入）
--
-- 支付单独成表而不是塞进交易表：支付的维度是支付方式，
-- 和交易的品类/大区完全不同，硬塞进同一张表会让两边的维度列
-- 互相填 'ALL'，Key 空间白白膨胀一倍。
-- ============================================================
CREATE TABLE IF NOT EXISTS dws.pay_window_agg (
    stat_date           DATE            NOT NULL              COMMENT '统计日期，分区列',
    window_type         VARCHAR(16)     NOT NULL              COMMENT 'TUMBLE_1M / CUMULATE_1D',
    window_start        DATETIME(3)     NOT NULL              COMMENT '窗口起点',
    payment_type        VARCHAR(32)     NOT NULL              COMMENT '支付方式，ALL 表示汇总',

    window_end          DATETIME(3)     NULL                  COMMENT '窗口终点',
    pay_cnt             BIGINT          NULL DEFAULT "0"      COMMENT '支付订单数',
    pay_user_cnt        BIGINT          NULL DEFAULT "0"      COMMENT '支付用户数',
    pay_amount          DECIMAL(18, 2)  NULL DEFAULT "0"      COMMENT '支付金额',

    max_lag_seconds     INT             NULL DEFAULT "0"      COMMENT '窗口内最大端到端延迟（秒）',
    avg_lag_seconds     INT             NULL DEFAULT "0"      COMMENT '窗口内平均端到端延迟（秒）',

    ingest_time         DATETIME(3)     NULL                  COMMENT 'Flink 落库时间'
) ENGINE = OLAP
UNIQUE KEY(stat_date, window_type, window_start, payment_type)
COMMENT '支付域窗口聚合 - DWS层（Flink 写入，主键幂等）'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(window_start) BUCKETS 6
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-15",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "6",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "15"
);


-- ============================================================
-- 用户日活 BITMAP（调度刷新写入，不是 Flink 写的）
--
-- 为什么独立用户数要单独建一张 BITMAP 表：
--
-- 窗口聚合表里的 order_user_cnt 是 Flink 在窗口状态里算的精确去重值，
-- 但它只在「那个窗口内」有效 —— 一个用户在 10:01 和 10:05 各下一单，
-- 两个分钟窗口各记一个用户，把这些行 SUM 起来会得到 2，而不是 1。
-- 跨窗口、跨天的去重，聚合结果本身根本合并不了。
--
-- 常规做法是回明细表 COUNT(DISTINCT user_id)：查一次扫一次全量，
-- 问「最近 30 天有多少独立用户」就得扫 30 个分区的全部明细。
--
-- BITMAP 的做法是把「当天有哪些用户」这个集合本身存下来。
-- 之后任意多天的独立用户数 = BITMAP_UNION_COUNT(uv_bitmap)，
-- Doris 在压缩位图上做或运算，不回明细、结果精确（不是 HLL 那种估算）。
-- 30 天 UV 从「扫 30 个分区明细」变成「合并 30 个位图」。
--
-- user_id 本身就是 BIGINT，直接 TO_BITMAP(user_id) 即可；
-- 只有字符串主键才需要先 BITMAP_HASH 映射成整型（那会引入极小的碰撞概率）。
-- ============================================================
CREATE TABLE IF NOT EXISTS dws.user_active_daily (
    dt                  DATE            NOT NULL              COMMENT '日期',
    region_name         VARCHAR(64)     NOT NULL              COMMENT '大区，ALL 表示全国',

    order_cnt           BIGINT          SUM DEFAULT "0"       COMMENT '订单数',
    order_amount        DECIMAL(18, 2)  SUM DEFAULT "0"       COMMENT '成交额',
    uv_bitmap           BITMAP          BITMAP_UNION          COMMENT '当日下单用户位图，可跨天精确合并',
    pay_uv_bitmap       BITMAP          BITMAP_UNION          COMMENT '当日支付用户位图'
) ENGINE = OLAP
AGGREGATE KEY(dt, region_name)
COMMENT '用户日活 - DWS层（BITMAP 精确去重，由 ADS 刷新任务写入）'
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(region_name) BUCKETS 4
-- ── Rollup：大区维度前缀索引 ──
-- base 表的前缀索引是 (dt, region_name)。只按大区过滤、不带日期的查询
-- （「华东区历史累计有多少独立用户」）走不到前缀索引，得扫全部分区。
-- 加一个以 region_name 打头的 rollup，Doris 会自动路由过去。
-- Aggregate 模型的 rollup 还会顺带把 dt 维度提前聚合掉，扫描量进一步下降。
--
-- 写在 CREATE TABLE 里而不是事后 ALTER TABLE ADD ROLLUP：
-- 后者不幂等，init 脚本第二次执行就会报 "Rollup index already exists"
-- 把整个初始化卡住；跟着 IF NOT EXISTS 一起建则天然可重复执行。
ROLLUP (
    rollup_by_region (region_name, order_cnt, order_amount, uv_bitmap, pay_uv_bitmap)
)
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    -- 单机 BE 下 tablet 总数要控制：这张表一天一个分区、4 个分桶，
    -- 历史分区开到 90 天就是 360 个 tablet，加上其他表容易把 BE 压出问题。
    -- 30 天足够覆盖「最近 30 天独立用户数」这类查询。
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "30"
);
