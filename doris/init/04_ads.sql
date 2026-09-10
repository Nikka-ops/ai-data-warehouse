-- ============================================================
-- 04 ADS 层：直接面向看板 / NL2SQL / Agent
--
-- 这一层刻意分成三类对象，因为不同问题对「新鲜度」和「响应速度」
-- 的要求根本不同：
--
--   普通 VIEW      查询时实时计算，永远是最新数据。
--                  用于当天几百行就能算出的当前状态卡片。
--   异步物化视图    预计算并落盘，定时刷新。
--                  用于要扫大量历史窗口的趋势图。
--   结果表         由独立调度任务写入（见 pipelines/refresh_ads.py）。
--                  用于跨天、需要 BITMAP 合并、算得比较重的排行榜。
--
-- 判断标准很简单：算一次的代价，乘以看板的刷新频率。
-- 秒级刷新的卡片必须便宜到能每次现算，重的东西一律预计算。
-- ============================================================


-- ============================================================
-- 今日累计交易 KPI（普通视图，查询即最新）
--
-- 看板顶部那排卡片用它。走 CUMULATE_1D 累计窗口的最新一行 ——
-- 累计值是 Flink 增量维护好的，这里只是把最新那一行取出来，
-- 一次点查就出结果，不扫明细也不做聚合。
--
-- 两个维度列同时约束成 'ALL'，取的是整体粒度那一行。
-- ============================================================
CREATE VIEW IF NOT EXISTS ads.v_today_trade_kpi AS
SELECT
    stat_date,
    window_end                                          AS as_of_time,
    order_cnt                                           AS today_order_cnt,
    order_user_cnt                                      AS today_order_user_cnt,
    sku_num                                             AS today_sku_num,
    order_amount                                        AS today_gmv,
    CASE WHEN order_cnt > 0
         THEN ROUND(order_amount / order_cnt, 2)
         ELSE 0 END                                     AS avg_order_value,
    max_order_price,
    abnormal_price_cnt,
    max_lag_seconds,
    avg_lag_seconds
FROM dws.trade_window_agg
WHERE window_type    = 'CUMULATE_1D'
  AND category1_name = 'ALL'
  AND region_name    = 'ALL'
  AND stat_date      = CURDATE()
ORDER BY window_start DESC
LIMIT 1;


-- ============================================================
-- 今日累计支付 KPI（普通视图）
-- ============================================================
CREATE VIEW IF NOT EXISTS ads.v_today_pay_kpi AS
SELECT
    stat_date,
    window_end                                          AS as_of_time,
    pay_cnt                                             AS today_pay_cnt,
    pay_user_cnt                                        AS today_pay_user_cnt,
    pay_amount                                          AS today_pay_amount,
    CASE WHEN pay_cnt > 0
         THEN ROUND(pay_amount / pay_cnt, 2)
         ELSE 0 END                                     AS avg_pay_value,
    max_lag_seconds,
    avg_lag_seconds
FROM dws.pay_window_agg
WHERE window_type  = 'CUMULATE_1D'
  AND payment_type = 'ALL'
  AND stat_date    = CURDATE()
ORDER BY window_start DESC
LIMIT 1;


-- ============================================================
-- 支付转化漏斗（普通视图）
--
-- 这个指标只能从累积快照算，算不了窗口聚合：
-- 「有多少订单当前处于已支付状态」问的是状态分布，不是事件计数。
-- 事务事实表每行是一次不可变的业务动作，回答不了「现在怎么样」。
--
-- 同时给出下单到支付的平均/中位时长 —— pay_lag_seconds 是快照表里
-- 跨状态时间差的产物，是运营判断支付流程有没有卡顿的直接依据。
-- ============================================================
CREATE VIEW IF NOT EXISTS ads.v_today_conversion AS
SELECT
    create_date                                                     AS stat_date,
    COUNT(*)                                                        AS order_cnt,
    SUM(CASE WHEN order_status IN ('PAID','SHIPPED','DELIVERED')
             THEN 1 ELSE 0 END)                                     AS paid_cnt,
    SUM(CASE WHEN order_status = 'SHIPPED'   THEN 1 ELSE 0 END)     AS shipped_cnt,
    SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END)     AS delivered_cnt,
    SUM(CASE WHEN order_status = 'CANCELED'  THEN 1 ELSE 0 END)     AS canceled_cnt,
    SUM(CASE WHEN order_status = 'REFUNDED'  THEN 1 ELSE 0 END)     AS refunded_cnt,
    ROUND(SUM(CASE WHEN order_status IN ('PAID','SHIPPED','DELIVERED')
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)        AS pay_conversion_pct,
    ROUND(SUM(CASE WHEN order_status = 'CANCELED' THEN 1 ELSE 0 END)
              * 100.0 / COUNT(*), 2)                                AS cancel_rate_pct,
    ROUND(AVG(pay_lag_seconds), 1)                                  AS avg_pay_lag_seconds,
    MAX(pay_lag_seconds)                                            AS max_pay_lag_seconds
FROM dwd.order_snapshot
WHERE create_date = CURDATE()
GROUP BY create_date;


-- ============================================================
-- 分钟趋势（普通视图）
-- 看板折线图用它，只看最近 2 小时 —— 120 行，现算完全够快
-- ============================================================
CREATE VIEW IF NOT EXISTS ads.v_minute_trend AS
SELECT
    window_start,
    window_end,
    order_cnt,
    order_user_cnt,
    sku_num,
    order_amount                                        AS gmv,
    CASE WHEN sku_num > 0
         THEN ROUND(order_amount / sku_num, 2)
         ELSE 0 END                                     AS avg_price,
    max_order_price,
    abnormal_price_cnt,
    max_lag_seconds,
    avg_lag_seconds
FROM dws.trade_window_agg
WHERE window_type    = 'TUMBLE_1M'
  AND category1_name = 'ALL'
  AND region_name    = 'ALL'
  AND window_start  >= DATE_SUB(NOW(), INTERVAL 2 HOUR);


-- ============================================================
-- 小时趋势（异步物化视图）
--
-- 从分钟窗口滚到小时，60:1 的压缩比。看板要画「今天每小时走势」时
-- 直接读这张预计算表，不用现场聚合 1440 行。
-- 每 5 分钟自动刷新一次 —— 趋势图不需要秒级新鲜度，
-- 需要秒级新鲜度的是上面那几个当前状态卡片，那些走的是普通视图。
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS ads.mv_hourly_trend
BUILD IMMEDIATE
REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
DISTRIBUTED BY HASH(stat_date) BUCKETS 2
PROPERTIES ("replication_num" = "1")
AS
SELECT
    stat_date,
    HOUR(window_start)                                  AS stat_hour,
    category1_name,
    region_name,
    SUM(order_cnt)                                      AS order_cnt,
    SUM(sku_num)                                        AS sku_num,
    SUM(order_amount)                                   AS gmv,
    SUM(abnormal_price_cnt)                             AS abnormal_price_cnt,
    MAX(max_order_price)                                AS max_order_price,
    MAX(max_lag_seconds)                                AS max_lag_seconds
FROM dws.trade_window_agg
WHERE window_type = 'TUMBLE_1M'
GROUP BY stat_date, HOUR(window_start), category1_name, region_name;


-- ============================================================
-- 品类排行（结果表，由 pipelines/refresh_ads.py 刷新）
--
-- 之所以不做成视图：排行要跨天回看，且要带 BITMAP 独立用户数，
-- 每次现算都要合并多天位图 + 排序。这类查询固化成结果表更划算，
-- 刷新频率交给调度决定，和常驻流作业完全解耦 ——
-- 刷新任务跑挂了，实时链路照常写入，看板只是排行榜旧了几分钟。
-- ============================================================
CREATE TABLE IF NOT EXISTS ads.category_rank (
    stat_date           DATE            NOT NULL              COMMENT '统计日期',
    category1_name      VARCHAR(64)     NOT NULL              COMMENT '一级品类',

    order_cnt           BIGINT          NULL DEFAULT "0"      COMMENT '订单数',
    sku_num             BIGINT          NULL DEFAULT "0"      COMMENT '商品件数',
    gmv                 DECIMAL(18, 2)  NULL DEFAULT "0"      COMMENT '成交额',
    avg_price           DECIMAL(16, 2)  NULL DEFAULT "0"      COMMENT '件均价',
    rank_by_gmv         INT             NULL DEFAULT "0"      COMMENT 'GMV 排名',
    gmv_share_pct       DECIMAL(10, 2)  NULL DEFAULT "0"      COMMENT 'GMV 占比 %',
    refresh_time        DATETIME        NULL                  COMMENT '刷新时间'
) ENGINE = OLAP
UNIQUE KEY(stat_date, category1_name)
COMMENT '品类销售排行 - ADS层（调度刷新）'
DISTRIBUTED BY HASH(stat_date) BUCKETS 2
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");


-- ============================================================
-- 地域排行（结果表，由 pipelines/refresh_ads.py 刷新）
-- uv 列直接来自 dws.user_active_daily 的 BITMAP_UNION_COUNT
-- ============================================================
CREATE TABLE IF NOT EXISTS ads.region_rank (
    stat_date           DATE            NOT NULL              COMMENT '统计日期',
    region_name         VARCHAR(64)     NOT NULL              COMMENT '大区',

    order_cnt           BIGINT          NULL DEFAULT "0"      COMMENT '订单数',
    gmv                 DECIMAL(18, 2)  NULL DEFAULT "0"      COMMENT '成交额',
    uv                  BIGINT          NULL DEFAULT "0"      COMMENT '独立下单用户数（BITMAP 精确去重）',
    pay_uv              BIGINT          NULL DEFAULT "0"      COMMENT '独立支付用户数',
    rank_by_gmv         INT             NULL DEFAULT "0"      COMMENT 'GMV 排名',
    refresh_time        DATETIME        NULL                  COMMENT '刷新时间'
) ENGINE = OLAP
UNIQUE KEY(stat_date, region_name)
COMMENT '地域销售排行 - ADS层（调度刷新）'
DISTRIBUTED BY HASH(stat_date) BUCKETS 2
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");


-- ============================================================
-- 对账结果（结果表，由 pipelines/reconcile.py 写入）
--
-- 实时链路最难自证的一件事是「数对不对」。
-- 对账任务定时拿 MySQL 业务库和 Doris 数仓的同口径数字比一遍，
-- 差异落这张表 —— 有了它，「数据一致性保证」才是可验证的说法，
-- 而不是一句设计上的声明。
-- ============================================================
CREATE TABLE IF NOT EXISTS ads.reconcile_result (
    check_date          DATE            NOT NULL              COMMENT '对账业务日期',
    check_item          VARCHAR(64)     NOT NULL              COMMENT '对账项',

    check_time          DATETIME        NULL                  COMMENT '对账执行时间',
    source_value        DECIMAL(20, 2)  NULL DEFAULT "0"      COMMENT '业务库（MySQL）值',
    target_value        DECIMAL(20, 2)  NULL DEFAULT "0"      COMMENT '数仓（Doris）值',
    diff_value          DECIMAL(20, 2)  NULL DEFAULT "0"      COMMENT '差异 = 数仓 - 业务库',
    diff_pct            DECIMAL(10, 4)  NULL DEFAULT "0"      COMMENT '差异占比 %',
    status              VARCHAR(16)     NULL                  COMMENT 'PASS / WARN / FAIL',
    detail              VARCHAR(500)    NULL                  COMMENT '说明'
) ENGINE = OLAP
UNIQUE KEY(check_date, check_item)
COMMENT '业务库与数仓对账结果 - ADS层'
DISTRIBUTED BY HASH(check_date) BUCKETS 2
PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
