-- ============================================================
-- Flink SQL 会话初始化：CDC 源 + Kafka 分层 + Doris 服务层
--
-- 用法：
--   sql-client.sh -i /opt/flink/sql/00_init.sql -f /opt/flink/sql/10_dwd_job.sql
--
-- ── 一个必须先想清楚的建模问题 ──────────────────────────────
--
-- CDC 读出来的是 changelog（+I / -U / +U / -D，带撤回语义），
-- 而窗口聚合 TVF（TUMBLE / CUMULATE）只接受 append-only 流，
-- 直接把 CDC 流喂给窗口会报 "doesn't support consuming update changes"。
--
-- 解法不是绕过报错，而是按数仓的方式把两类表区分开：
--
--   事务事实表  ← 来自 insert-only 的业务表
--                （order_detail / payment_info / refund_info）
--                 一次业务动作写一行、之后不改，天然 append-only，
--                 可以安全地做窗口聚合。
--
--   累积快照    ← 来自会反复 UPDATE 的订单主表 order_info
--                 记录的是「订单当前状态」，本质是 changelog，
--                 用 upsert 语义落到 Doris Unique 表，不参与窗口聚合。
--
-- 这也是真实电商实时数仓的标准分法：GMV / 支付额这类指标算的是
-- 「事件发生了多少次、多少钱」，而不是「当前有多少订单处于某状态」。
-- ============================================================


-- ============================================================
-- 一、运行时参数
-- ============================================================

-- 兜底作业名；两个作业文件各自会覆盖成 rtdw-dwd / rtdw-dws，
-- 这样 flink list 和 submit.ps1 -Stop 才分得清该停哪个。
SET 'pipeline.name' = 'realtime-dw-mall';
-- 并行度 2 而不是 3：主链路是两个作业，2×2=4 个 slot，
-- 单 TaskManager（6 slot）装得下，还留出重启时的余量。
-- 设成 3 的话两个作业要 6 个 slot 且刚好占满，DWS 一重启就会
-- 抢不到资源报 NoResourceAvailableException。
-- 本地单机最小可跑：并行度 1。
-- 一个 TaskManager 只有这么多 slot，DWD 作业里四个 sink 共用一份 slot 组，
-- 并行度调高只会让作业因为 NoResourceAvailableException 一直 RESTARTING。
SET 'parallelism.default' = '1';

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '10min';
SET 'execution.checkpointing.min-pause' = '10s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'table.exec.state.ttl' = '2h';

-- 空闲分区不推进 watermark 会让全局窗口卡死，必须设
SET 'table.exec.source.idle-timeout' = '30s';

SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2s';
SET 'table.exec.mini-batch.size' = '5000';

SET 'restart-strategy' = 'exponential-delay';
SET 'restart-strategy.exponential-delay.initial-backoff' = '10s';
SET 'restart-strategy.exponential-delay.max-backoff' = '2min';


-- ============================================================
-- 二、CDC 源：直读业务库 binlog
--
-- 这些表的定义要和 MySQL 侧严格对齐。几个关键配置：
--   server-id      binlog 复制协议要求唯一；配成区间是为了让
--                  多并行度的 source 每个子任务各占一个 id
--   scan.startup.mode = initial
--                  先全量快照一次，再无缝切到 binlog 增量，
--                  这样下游不需要额外做一次历史数据初始化
--   scan.incremental.snapshot.enabled
--                  增量快照算法：全量阶段可并行、可 checkpoint，
--                  且不锁表 —— 生产环境接大表时这一条是刚需
-- ============================================================

-- ── 订单主表（changelog，用作累积快照）──
CREATE TEMPORARY TABLE ods_order_info (
    id                  BIGINT,
    user_id             BIGINT,
    province_id         INT,
    order_status        STRING,
    total_amount        DECIMAL(16, 2),
    activity_reduce     DECIMAL(16, 2),
    coupon_reduce       DECIMAL(16, 2),
    freight_amount      DECIMAL(16, 2),
    create_time         TIMESTAMP(3),
    payment_time        TIMESTAMP(3),
    ship_time           TIMESTAMP(3),
    receive_time        TIMESTAMP(3),
    cancel_time         TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    -- lookup join 的时态表语义要求左表带一个「处理时间属性」列。
    -- 不能在 join 里直接写 FOR SYSTEM_TIME AS OF PROCTIME() ——
    -- Flink 会报 Unsupported time travel expression，因为内联的 PROCTIME()
    -- 只是个函数调用，规约不成常量，构不成时间属性。必须像这样先声明成列。
    proc_time           AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'mysql-cdc',
    'hostname'   = 'mysql',
    'port'       = '3306',
    'username'   = 'root',
    'password'   = 'root123',
    'database-name' = 'mall',
    'table-name'    = 'order_info',
    'server-id'     = '5400-5405',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'server-time-zone'  = 'Asia/Shanghai'
);

-- ── 订单明细（insert-only → 下单事务事实）──
CREATE TEMPORARY TABLE ods_order_detail (
    id                  BIGINT,
    order_id            BIGINT,
    sku_id              BIGINT,
    sku_num             INT,
    order_price         DECIMAL(16, 2),
    split_total_amount  DECIMAL(16, 2),
    create_time         TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    proc_time           AS PROCTIME(),
    -- 下单时间即事件时间；容忍 5 分钟乱序
    WATERMARK FOR create_time AS create_time - INTERVAL '5' MINUTE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'mysql-cdc',
    'hostname'   = 'mysql',
    'port'       = '3306',
    'username'   = 'root',
    'password'   = 'root123',
    'database-name' = 'mall',
    'table-name'    = 'order_detail',
    'server-id'     = '5410-5415',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'server-time-zone'  = 'Asia/Shanghai'
);

-- ── 支付表（支付事务事实）──
CREATE TEMPORARY TABLE ods_payment_info (
    id                  BIGINT,
    order_id            BIGINT,
    user_id             BIGINT,
    payment_type        STRING,
    payment_status      STRING,
    payment_amount      DECIMAL(16, 2),
    callback_time       TIMESTAMP(3),
    create_time         TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    proc_time           AS PROCTIME(),
    WATERMARK FOR callback_time AS callback_time - INTERVAL '5' MINUTE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'mysql-cdc',
    'hostname'   = 'mysql',
    'port'       = '3306',
    'username'   = 'root',
    'password'   = 'root123',
    'database-name' = 'mall',
    'table-name'    = 'payment_info',
    'server-id'     = '5420-5425',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'server-time-zone'  = 'Asia/Shanghai'
);

-- ── 退款表（退款事务事实）──
CREATE TEMPORARY TABLE ods_refund_info (
    id                  BIGINT,
    order_id            BIGINT,
    user_id             BIGINT,
    refund_type         STRING,
    refund_status       STRING,
    refund_amount       DECIMAL(16, 2),
    refund_reason       STRING,
    create_time         TIMESTAMP(3),
    update_time         TIMESTAMP(3),
    proc_time           AS PROCTIME(),
    WATERMARK FOR create_time AS create_time - INTERVAL '5' MINUTE,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'mysql-cdc',
    'hostname'   = 'mysql',
    'port'       = '3306',
    'username'   = 'root',
    'password'   = 'root123',
    'database-name' = 'mall',
    'table-name'    = 'refund_info',
    'server-id'     = '5430-5435',
    'scan.startup.mode' = 'initial',
    'scan.incremental.snapshot.enabled' = 'true',
    'server-time-zone'  = 'Asia/Shanghai'
);


-- ============================================================
-- 三、维表：JDBC lookup source
--
-- 这里有一个必须讲清楚的取舍，一开始写成 mysql-cdc 是错的。
--
-- 维表用 mysql-cdc 看起来很美：变更实时感知，不需要定时全量同步。
-- 但 CDC source 不实现 LookupTableSource，写 FOR SYSTEM_TIME AS OF 时
-- Flink 只能把它规划成「changelog 上的时态表 join」，于是整条流带上撤回语义，
-- 下游普通 Kafka sink 直接报 doesn't support consuming update and delete changes。
--
-- 换成 JDBC lookup source 之后语义变成「每条数据来的时候点查一次维表」：
-- 输出保持 append-only，状态里也不需要常驻整张维表。
-- 代价是维表变更的感知延迟等于缓存 TTL —— 所以 TTL 按各维表的变化频率分别设：
--   sku       会改价、会下架，1 分钟
--   品类/省份  基本不变，30 分钟
--
-- ============================================================

-- ── 订单主表的 lookup 视角 ──
--
-- 这张表和前面的 ods_order_info 指向同一张 MySQL 表，但用途完全不同，
-- 不能互相替代：
--
--   ods_order_info（mysql-cdc）  拿 changelog，喂订单累积快照，
--                                关心的是「状态怎么变的」。
--   dim_order_info（jdbc）       拿「此刻的那一行」，给订单明细补
--                                user_id / province_id —— 这两个字段
--                                下单之后就不再变，点查一次即可。
CREATE TEMPORARY TABLE dim_order_info (
    id                  BIGINT,
    user_id             BIGINT,
    province_id         INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:mysql://mysql:3306/mall?serverTimezone=Asia/Shanghai',
    'table-name' = 'order_info',
    'username'   = 'root',
    'password'   = 'root123',
    'lookup.cache'                            = 'PARTIAL',
    'lookup.partial-cache.max-rows'           = '50000',
    'lookup.partial-cache.expire-after-write' = '10min',
    'lookup.max-retries'                      = '3'
);

CREATE TEMPORARY TABLE dim_sku (
    id                  BIGINT,
    sku_name            STRING,
    category3_id        INT,
    tm_id               INT,
    price               DECIMAL(16, 2),
    is_sale             TINYINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:mysql://mysql:3306/mall?serverTimezone=Asia/Shanghai',
    'table-name' = 'sku_info',
    'username'   = 'root',
    'password'   = 'root123',
    'lookup.cache'                            = 'PARTIAL',
    'lookup.partial-cache.max-rows'           = '20000',
    'lookup.partial-cache.expire-after-write' = '1min',
    'lookup.max-retries'                      = '3'
);

CREATE TEMPORARY TABLE dim_category (
    id                  INT,
    name                STRING,
    category2_id        INT,
    category2_name      STRING,
    category1_name      STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:mysql://mysql:3306/mall?serverTimezone=Asia/Shanghai',
    'table-name' = 'base_category3',
    'username'   = 'root',
    'password'   = 'root123',
    'lookup.cache'                            = 'PARTIAL',
    'lookup.partial-cache.max-rows'           = '5000',
    'lookup.partial-cache.expire-after-write' = '30min',
    'lookup.max-retries'                      = '3'
);

CREATE TEMPORARY TABLE dim_province (
    id                  INT,
    name                STRING,
    region_id           INT,
    region_name         STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'  = 'jdbc',
    'url'        = 'jdbc:mysql://mysql:3306/mall?serverTimezone=Asia/Shanghai',
    'table-name' = 'base_province',
    'username'   = 'root',
    'password'   = 'root123',
    'lookup.cache'                            = 'PARTIAL',
    'lookup.partial-cache.max-rows'           = '5000',
    'lookup.partial-cache.expire-after-write' = '30min',
    'lookup.max-retries'                      = '3'
);


-- ============================================================
-- 四、DWD 层落在 Kafka
--
-- 分层不落库、落 Kafka，是实时数仓和离线数仓最大的形态差异：
--   * 每落一次库就多一次写入和读取，延迟逐层累加
--   * DWD 是给下游流作业消费的，不是给人查的，没必要进 OLAP
--   * 多个 DWS 作业可以共享同一份 DWD topic，避免重复清洗
--
-- ── 为什么写用 upsert-kafka、读用普通 kafka ──────────────────
--
-- 这一对不对称的选择是被 changelog 逼出来的，值得说清楚。
--
-- mysql-cdc source 的 changelog 模式固定是 ALL（+I/-U/+U/-D），
-- 哪怕业务表 insert-only、运行时一条 UPDATE 都不会来，规划器看到的
-- 仍然是 upsert 流。而普通 kafka sink 只收 append-only，于是报
-- "doesn't support consuming update and delete changes"。
--
-- 试图在中间「转成 append-only」是走不通的：Deduplicate 算子本身
-- 同样不接受 changelog 输入，套一层去重只会把报错换个位置。
--
-- 正确的做法是让写入端承认这是 changelog：upsert-kafka 按主键写，
-- 同一主键的新版本覆盖旧版本，语义上和 Doris 的 Unique Key 是一回事。
--
-- 读回来时用普通 kafka + json：这个组合在类型系统里就是 append-only 源，
-- 窗口 TVF 才能消费。语义上也站得住 —— 这三张是事务事实表，
-- 一次业务动作写一行、之后不改，topic 里实际只会有 +I，
-- 「按主键覆盖」和「一条事件」在这里是同一件事。
--
-- 写入端开 exactly-once：Kafka 事务与 checkpoint 对齐，故障恢复时
-- 未提交的事务被丢弃，下游读 read_committed 就看不到重复。
-- 这一环很关键 —— 窗口聚合是 SUM，重复一条就是多算一笔 GMV，
-- 光靠 Doris 主键幂等兜不住（幂等保的是同一主键的重复写，
-- 不是同一笔金额被算两次）。
-- ============================================================

CREATE TEMPORARY TABLE dwd_trade_order (
    order_detail_id     BIGINT,
    order_id            BIGINT,
    user_id             BIGINT,
    sku_id              BIGINT,
    sku_name            STRING,
    category3_id        INT,
    category3_name      STRING,
    category1_name      STRING,
    province_id         INT,
    province_name       STRING,
    region_name         STRING,
    sku_num             INT,
    order_price         DECIMAL(16, 2),
    split_amount        DECIMAL(16, 2),
    is_price_abnormal   TINYINT,
    create_time         TIMESTAMP(3),
    -- 端到端延迟：事件在业务库发生 → Flink 加工完成 的秒数。
    -- 在 DWD 层算而不是 DWS 层，是因为它衡量的是「这条数据走了多久」，
    -- 属于单条记录的属性；到了 DWS 只需要对它做 MAX / AVG。
    lag_seconds         INT,
    ingest_time         TIMESTAMP(3),
    PRIMARY KEY (order_detail_id) NOT ENFORCED
) WITH (
    'connector'    = 'upsert-kafka',
    'topic'        = 'dwd_trade_order',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format'   = 'json',
    'value.format' = 'json',
    -- Kafka 事务与 checkpoint 对齐：故障恢复时未提交的事务被丢弃，
    -- 下游以 read_committed 读就不会看到重复数据
    'sink.delivery-guarantee'      = 'exactly-once',
    'sink.transactional-id-prefix' = 'rtdw-dwd-order'
);

CREATE TEMPORARY TABLE dwd_trade_pay (
    payment_id          BIGINT,
    order_id            BIGINT,
    user_id             BIGINT,
    payment_type        STRING,
    payment_amount      DECIMAL(16, 2),
    callback_time       TIMESTAMP(3),
    lag_seconds         INT,
    ingest_time         TIMESTAMP(3),
    PRIMARY KEY (payment_id) NOT ENFORCED
) WITH (
    'connector'    = 'upsert-kafka',
    'topic'        = 'dwd_trade_pay',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format'   = 'json',
    'value.format' = 'json',
    -- Kafka 事务与 checkpoint 对齐：故障恢复时未提交的事务被丢弃，
    -- 下游以 read_committed 读就不会看到重复数据
    'sink.delivery-guarantee'      = 'exactly-once',
    'sink.transactional-id-prefix' = 'rtdw-dwd-pay'
);

CREATE TEMPORARY TABLE dwd_trade_refund (
    refund_id           BIGINT,
    order_id            BIGINT,
    user_id             BIGINT,
    refund_type         STRING,
    refund_amount       DECIMAL(16, 2),
    refund_reason       STRING,
    create_time         TIMESTAMP(3),
    ingest_time         TIMESTAMP(3),
    PRIMARY KEY (refund_id) NOT ENFORCED
) WITH (
    'connector'    = 'upsert-kafka',
    'topic'        = 'dwd_trade_refund',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format'   = 'json',
    'value.format' = 'json',
    -- Kafka 事务与 checkpoint 对齐：故障恢复时未提交的事务被丢弃，
    -- 下游以 read_committed 读就不会看到重复数据
    'sink.delivery-guarantee'      = 'exactly-once',
    'sink.transactional-id-prefix' = 'rtdw-dwd-refund'
);


-- ── DWD topic 的消费端定义（供 DWS 作业读回，带 watermark）──
CREATE TEMPORARY TABLE dwd_trade_order_src (
    order_detail_id     BIGINT,
    order_id            BIGINT,
    user_id             BIGINT,
    sku_id              BIGINT,
    sku_name            STRING,
    category3_id        INT,
    category3_name      STRING,
    category1_name      STRING,
    province_id         INT,
    province_name       STRING,
    region_name         STRING,
    sku_num             INT,
    order_price         DECIMAL(16, 2),
    split_amount        DECIMAL(16, 2),
    is_price_abnormal   TINYINT,
    create_time         TIMESTAMP(3),
    lag_seconds         INT,
    ingest_time         TIMESTAMP(3),
    -- Kafka 元数据虚拟列：迟到数据分流时要把 partition/offset 一起存下来，
    -- 否则「可回放」只是一句空话 —— 位点才是回放的唯一凭据。
    `kafka_partition`   INT    METADATA FROM 'partition' VIRTUAL,
    `kafka_offset`      BIGINT METADATA FROM 'offset'    VIRTUAL,
    WATERMARK FOR create_time AS create_time - INTERVAL '5' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic'     = 'dwd_trade_order',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_dws_trade',
    'scan.startup.mode'   = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    -- 只读已提交的事务，配合写入端的 exactly-once 才完整
    'properties.isolation.level' = 'read_committed',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY TABLE dwd_trade_pay_src (
    payment_id          BIGINT,
    order_id            BIGINT,
    user_id             BIGINT,
    payment_type        STRING,
    payment_amount      DECIMAL(16, 2),
    callback_time       TIMESTAMP(3),
    lag_seconds         INT,
    ingest_time         TIMESTAMP(3),
    WATERMARK FOR callback_time AS callback_time - INTERVAL '5' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic'     = 'dwd_trade_pay',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_dws_trade',
    'scan.startup.mode'   = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    -- 只读已提交的事务，配合写入端的 exactly-once 才完整
    'properties.isolation.level' = 'read_committed',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);


-- ============================================================
-- 五、Doris 服务层
--
-- ── 关于 __RUNID__ ──
-- Doris 连接器开 2PC 后，导入 label = {label-prefix}_{子任务号}_{checkpoint号}。
-- Doris 靠 label 做导入去重：同一个 label 提交两次，第二次直接被拒。
-- 这正是导入幂等的实现方式，但它有个副作用：作业被 cancel 之后重新提交，
-- checkpoint 号又从 1 开始数，label 于是和上一轮完全撞上，
-- 报 "Label Already Exists and load job finished"，TaskManager 直接退出。
--
-- 连接器给的两条出路是「从 savepoint 恢复」或「换 label 前缀」。
-- 前者适用于正常运维（submit.ps1 -Stop 就是打 savepoint 停），
-- 后者适用于重新部署 —— submit.ps1 在提交前把 __RUNID__ 替换成本次运行的
-- 时间戳，于是每次全新提交都有独立的 label 空间，而同一次运行内
-- 从 checkpoint 自动恢复时前缀保持不变，幂等性仍然成立。
--
-- 只有需要被人和 BI 查询的层才进 Doris：
--   订单累积快照（明细查询、状态分布）
--   DWS 窗口聚合（看板指标）
-- ── 一致性语义：为什么这里不开 2PC ──
--
-- Doris 连接器的两阶段提交能做到 sink 侧 Exactly-Once，但它依赖
-- 「label 在 Doris 侧全局唯一且状态干净」这个前提。作业每次全新提交、
-- 失败重启、事务被中止之后，连接器都要去清理上一轮遗留的事务，
-- 这个路径在本地反复起停的场景下很容易撞上
-- "Exist label abort finished, retry"，导致 checkpoint 1 就失败，
-- 之后每次重启又从 checkpoint 1 开始，陷入死循环。
--
-- 这里改成不开 2PC，一致性由下游模型来保证，而不是靠事务：
--   * 所有 Doris 目标表都是 Unique Key + Merge-on-Write，按主键覆盖；
--   * 订单快照表还配了 update_time 作为 sequence 列，迟到的旧状态
--     不会覆盖新状态。
-- 于是同一批数据无论被重放几次，最终结果都收敛到同一个值 ——
-- 语义上是 at-least-once 投递 + 幂等写入 = effectively-once，
-- 这也是实时数仓里比纯 2PC 更常见、更耐操的做法。
-- Flink 内部状态仍然是 EXACTLY_ONCE（checkpoint 保证），没有降级。
-- ============================================================

CREATE TEMPORARY TABLE sink_dwd_order_snapshot (
    create_date         DATE,
    order_id            BIGINT,
    update_time         TIMESTAMP(3),
    user_id             BIGINT,
    province_id         INT,
    province_name       STRING,
    region_name         STRING,
    order_status        STRING,
    total_amount        DECIMAL(16, 2),
    activity_reduce     DECIMAL(16, 2),
    coupon_reduce       DECIMAL(16, 2),
    freight_amount      DECIMAL(16, 2),
    create_time         TIMESTAMP(3),
    payment_time        TIMESTAMP(3),
    ship_time           TIMESTAMP(3),
    receive_time        TIMESTAMP(3),
    pay_lag_seconds     INT,
    ingest_time         TIMESTAMP(3)
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'doris-fe:8030',
    'table.identifier'                  = 'dwd.order_snapshot',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'rtdw_order_snapshot___RUNID__',
    'sink.enable-2pc'                   = 'false',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    -- 订单会被反复更新，用 update_time 做 sequence 列，
    -- 保证迟到的旧状态不会覆盖新状态
    'sink.properties.function_column.sequence_col' = 'update_time',
    'sink.buffer-flush.max-rows'        = '20000',
    'sink.buffer-flush.interval'        = '10s',
    'sink.max-retries'                  = '3'
);

CREATE TEMPORARY TABLE sink_dws_trade_window (
    stat_date           DATE,
    window_type         STRING,
    window_start        TIMESTAMP(3),
    category1_name      STRING,
    region_name         STRING,
    window_end          TIMESTAMP(3),
    order_cnt           BIGINT,
    order_user_cnt      BIGINT,
    sku_num             BIGINT,
    order_amount        DECIMAL(18, 2),
    max_order_price     DECIMAL(16, 2),
    abnormal_price_cnt  BIGINT,
    max_lag_seconds     INT,
    avg_lag_seconds     INT,
    ingest_time         TIMESTAMP(3)
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'doris-fe:8030',
    'table.identifier'                  = 'dws.trade_window_agg',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'rtdw_dws_trade___RUNID__',
    'sink.enable-2pc'                   = 'false',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.buffer-flush.max-rows'        = '10000',
    'sink.buffer-flush.interval'        = '10s',
    'sink.max-retries'                  = '3'
);

CREATE TEMPORARY TABLE sink_dws_pay_window (
    stat_date           DATE,
    window_type         STRING,
    window_start        TIMESTAMP(3),
    payment_type        STRING,
    window_end          TIMESTAMP(3),
    pay_cnt             BIGINT,
    pay_user_cnt        BIGINT,
    pay_amount          DECIMAL(18, 2),
    max_lag_seconds     INT,
    avg_lag_seconds     INT,
    ingest_time         TIMESTAMP(3)
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'doris-fe:8030',
    'table.identifier'                  = 'dws.pay_window_agg',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'rtdw_dws_pay___RUNID__',
    'sink.enable-2pc'                   = 'false',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.buffer-flush.max-rows'        = '10000',
    'sink.buffer-flush.interval'        = '10s',
    'sink.max-retries'                  = '3'
);


-- ── 迟到数据兜底 ──
--
-- watermark 只给了 5 分钟乱序容忍，超出的记录窗口已经关闭、进不去了。
-- 不做处理的话它们就是静默丢失：不进结果，也没有记录，连丢了多少都不知道。
-- 分流到这张表之后，「丢数」变成「可量化、可按 Kafka 位点回放、可补算对账」。
CREATE TEMPORARY TABLE sink_stream_late_records (
    late_date           DATE,
    detect_time         TIMESTAMP(3),
    record_key          STRING,
    source_table        STRING,
    event_time          TIMESTAMP(3),
    watermark_time      TIMESTAMP(3),
    lateness_seconds    INT,
    kafka_partition     INT,
    kafka_offset        BIGINT,
    order_id            BIGINT,
    split_amount        DECIMAL(16, 2),
    is_compensated      TINYINT
) WITH (
    'connector'                         = 'doris',
    'fenodes'                           = 'doris-fe:8030',
    'table.identifier'                  = 'stream.late_records',
    'username'                          = 'root',
    'password'                          = '',
    'sink.label-prefix'                 = 'rtdw_late_records___RUNID__',
    'sink.enable-2pc'                   = 'false',
    'sink.properties.format'            = 'json',
    'sink.properties.read_json_by_line' = 'true',
    -- 迟到是小概率事件，攒不满大批次，所以靠 flush 间隔而不是行数来触发。
    -- max-rows 不能再往下调了：flink-doris-connector 强制要求 >= 10000
    -- （小于这个值直接抛 bufferFlushMaxRows must be greater than or equal to 10000），
    -- 它的用意正是防止高频小批量导入把 Doris 打出一堆小版本、压垮 compaction。
    'sink.buffer-flush.max-rows'        = '10000',
    'sink.buffer-flush.interval'        = '30s',
    'sink.max-retries'                  = '3'
);
