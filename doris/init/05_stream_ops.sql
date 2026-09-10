-- ============================================================
-- 05 流处理运维元数据
--
-- 这几张表回答的是「实时链路自己健不健康」，不是业务问题。
-- 没有这一层的话，作业挂了、数据丢了、延迟涨了，都只能靠肉眼盯控制台，
-- 事后什么都查不到。有了它，链路健康度和业务指标一样可以用 SQL 查、
-- 在看板上画、被 Agent 读走做诊断。
-- ============================================================


-- ============================================================
-- 迟到数据兜底表（Flink 侧写入）
--
-- 这张表是整条链路里最该讲清楚的一张。
--
-- 业务库的变更经过 Kafka 多分区、Flink 多并行度之后，到达顺序不再保证。
-- watermark 设了 5 分钟乱序容忍：这个范围内的乱序数据能正常进窗口参与聚合。
-- 但超出 5 分钟的数据，窗口已经触发并关闭，它进不去了。
--
-- 如果什么都不做，这部分数据就是静默丢失 —— 不进聚合结果，
-- 也没有任何记录，连「丢了多少」都统计不出来。
--
-- 做法是在 DWS 作业里用 CURRENT_WATERMARK(create_time) 显式判定：
-- 事件时间已经落在当前 watermark 之后的记录，单独分流写到这张表，
-- 连同 Kafka 的 partition / offset 一起存。于是「丢数」变成
-- 「进了兜底表，可量化、可按位点精确回放、可补算对账」。
--
-- 顺带一个取舍：Flink 1.18 的窗口 TVF 没有 allowed-lateness 参数，
-- 乱序容忍度完全由 watermark 延迟决定 —— 设大了窗口出结果慢，
-- 设小了落到兜底表的数据多。5 分钟是按上游乱序分布定的。
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.late_records (
    late_date           DATE            NOT NULL              COMMENT '事件日期，分区列',
    detect_time         DATETIME(3)     NOT NULL              COMMENT '被判定为迟到的时刻',
    record_key          VARCHAR(64)     NOT NULL              COMMENT '业务主键（order_detail_id）',

    source_table        VARCHAR(64)     NULL                  COMMENT '来源 topic',
    event_time          DATETIME(3)     NULL                  COMMENT '消息自带的事件时间',
    watermark_time      DATETIME(3)     NULL                  COMMENT '判定当时的 watermark',
    lateness_seconds    INT             NULL DEFAULT "0"      COMMENT '迟到秒数 = watermark - event_time',
    kafka_partition     INT             NULL                  COMMENT 'Kafka 分区号',
    kafka_offset        BIGINT          NULL                  COMMENT 'Kafka 位点，可据此精确回放这条消息',
    order_id            BIGINT          NULL                  COMMENT '订单ID',
    split_amount        DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '该条明细的成交额，用于估算影响面',
    is_compensated      TINYINT         NULL DEFAULT "0"      COMMENT '是否已被补算回聚合结果'
) ENGINE = OLAP
DUPLICATE KEY(late_date, detect_time, record_key)
COMMENT '迟到数据兜底表（Flink 侧分流写入）'
PARTITION BY RANGE(late_date) ()
DISTRIBUTED BY HASH(record_key) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "30"
);


-- ============================================================
-- AI 质检告警表
--
-- 设计要点是「规则先判、命中才调 LLM」：正常窗口 0 次 LLM 调用。
-- 另外两件事：
--   1) alert_key + Unique Key 去重 —— 同一个窗口的同一种异常反复触发时
--      不刷屏，只保留最新一条（alert_time 做 sequence）
--   2) 窗口上下文留档 —— 当时的指标值和基线都存下来，
--      事后能复盘「这条告警到底是不是误报」
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.ai_quality_alerts (
    alert_date          DATE            NOT NULL              COMMENT '告警日期，分区列',
    alert_key           VARCHAR(128)    NOT NULL              COMMENT '去重键 = 窗口+类型+字段',

    alert_time          DATETIME(3)     NULL                  COMMENT '告警时间（sequence column）',
    alert_type          VARCHAR(32)     NULL                  COMMENT 'ANOMALY / QUALITY / LATENCY / PATTERN',
    severity            VARCHAR(16)     NULL                  COMMENT 'HIGH / MEDIUM / LOW',
    table_name          VARCHAR(64)     NULL                  COMMENT '涉及的表',
    field_name          VARCHAR(64)     NULL                  COMMENT '涉及的字段',
    detail              VARCHAR(1000)   NULL                  COMMENT '规则命中的描述',
    ai_suggestion       VARCHAR(2000)   NULL                  COMMENT 'LLM 给出的原因分析与处置建议',
    window_start        DATETIME(3)     NULL                  COMMENT '窗口起点',
    window_end          DATETIME(3)     NULL                  COMMENT '窗口终点',
    metric_value        DECIMAL(18, 4)  NULL DEFAULT "0"      COMMENT '当时的指标值',
    baseline_value      DECIMAL(18, 4)  NULL DEFAULT "0"      COMMENT '当时的基线值',
    threshold_value     DECIMAL(18, 4)  NULL DEFAULT "0"      COMMENT '触发阈值',
    llm_called          TINYINT         NULL DEFAULT "0"      COMMENT '本条是否真的调了 LLM'
) ENGINE = OLAP
UNIQUE KEY(alert_date, alert_key)
COMMENT 'AI 实时质检告警表'
PARTITION BY RANGE(alert_date) ()
DISTRIBUTED BY HASH(alert_key) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "alert_time",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "30"
);


-- ============================================================
-- Flink 作业运行指标
--
-- 由 ops_agent/collect.py 定时从 Flink REST API 抓取后写入。
-- 「你怎么知道作业有没有背压」这个问题，答案就是这张表：
-- busy_time 接近 1000ms/s 即算子满负荷，配合 checkpoint 耗时和
-- 重启次数一起看，才能区分是真背压还是单纯流量涨了。
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.job_metrics (
    stat_date               DATE            NOT NULL          COMMENT '日期，分区列',
    collect_time            DATETIME(3)     NOT NULL          COMMENT '采集时刻',
    job_name                VARCHAR(128)    NOT NULL          COMMENT 'Flink 作业名',

    job_id                  VARCHAR(64)     NULL              COMMENT 'Flink JobID',
    job_state               VARCHAR(32)     NULL              COMMENT 'RUNNING / FAILING / RESTARTING ...',
    uptime_seconds          BIGINT          NULL DEFAULT "0"  COMMENT '已运行秒数',
    parallelism             INT             NULL DEFAULT "0"  COMMENT '并行度',

    records_in_per_sec      DECIMAL(18, 2)  NULL DEFAULT "0"  COMMENT '入流速率',
    records_out_per_sec     DECIMAL(18, 2)  NULL DEFAULT "0"  COMMENT '出流速率',
    kafka_consumer_lag      BIGINT          NULL DEFAULT "0"  COMMENT 'Kafka 消费积压条数',

    busy_time_ms_per_sec    DECIMAL(10, 2)  NULL DEFAULT "0"  COMMENT '算子繁忙度，接近1000即背压',
    backpressure_level      VARCHAR(16)     NULL              COMMENT 'OK / LOW / HIGH',
    current_watermark_lag   BIGINT          NULL DEFAULT "0"  COMMENT 'watermark 落后当前时间的毫秒数',

    last_ckpt_duration_ms   BIGINT          NULL DEFAULT "0"  COMMENT '最近一次 checkpoint 耗时',
    last_ckpt_size_bytes    BIGINT          NULL DEFAULT "0"  COMMENT '最近一次 checkpoint 大小',
    ckpt_failed_count       BIGINT          NULL DEFAULT "0"  COMMENT '累计失败次数',
    restart_count           BIGINT          NULL DEFAULT "0"  COMMENT '累计重启次数'
) ENGINE = OLAP
DUPLICATE KEY(stat_date, collect_time, job_name)
COMMENT 'Flink 作业运行指标'
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(job_name) BUCKETS 2
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-15",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "2",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "15"
);


-- ============================================================
-- 实时 AI 风控结果（由 flink/ai_risk 算子写入）
--
-- 只存被判定为需复核（REVIEW）的订单，正常放行的不落库 ——
-- 否则风控表会被绝大多数正常订单淹没，查起来全是噪声。
--
-- llm_called 是刻意留的：用来事后统计「规则先筛」到底省了多少 LLM 调用。
-- 正常流量下这个值绝大部分是 0，只有命中规则的少数订单才是 1。
-- ============================================================
CREATE TABLE IF NOT EXISTS stream.ai_risk_result (
    judge_date          DATE            NOT NULL              COMMENT '研判日期，分区列',
    order_id            BIGINT          NOT NULL              COMMENT '订单ID',

    judge_time          DATETIME(3)     NULL                  COMMENT '研判时间（sequence 列）',
    user_id             BIGINT          NULL                  COMMENT '用户ID',
    sku_name            VARCHAR(128)    NULL                  COMMENT '商品名',
    split_amount        DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '订单金额',

    risk_action         VARCHAR(16)     NULL                  COMMENT 'PASS / REVIEW',
    risk_level          VARCHAR(16)     NULL                  COMMENT 'HIGH / MEDIUM / LOW',
    risk_reason         VARCHAR(256)    NULL                  COMMENT '风险理由（规则或 LLM 给出）',
    llm_called          TINYINT         NULL DEFAULT "0"      COMMENT '本条是否真的调了 LLM'
) ENGINE = OLAP
UNIQUE KEY(judge_date, order_id)
COMMENT '实时 AI 风控研判结果'
PARTITION BY RANGE(judge_date) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "judge_time",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "30"
);
