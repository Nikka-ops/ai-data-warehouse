-- ============================================================
-- DWS 层作业：Kafka DWD → 窗口聚合 → Doris
--
--   dwd_trade_order (Kafka) ──> TUMBLE 1min + CUMULATE 1day ──> dws.trade_window_agg
--                           └─> 迟到分流（CURRENT_WATERMARK）  ──> stream.late_records
--   dwd_trade_pay   (Kafka) ──> TUMBLE 1min + CUMULATE 1day ──> dws.pay_window_agg
--
-- 之所以能安心用窗口 TVF：这两个 topic 装的都是事务事实，append-only，
-- 没有撤回语义。这正是 DWD 层要把「事件」和「状态快照」分开建模的原因 ——
-- 订单主表那种会反复 UPDATE 的 changelog 喂进窗口会直接报
-- "doesn't support consuming update changes"。
--
-- 提交：sql-client.sh -i 00_init.sql -f 20_dws_job.sql
-- ============================================================

SET 'pipeline.name' = 'rtdw-dws';

EXECUTE STATEMENT SET
BEGIN

-- ── 1. 交易域分钟窗口 ───────────────────────────────────────
--
-- GROUPING SETS 一次算出三个粒度：整体 / 一级品类 / 大区。
-- 不用它就得开三份窗口状态、把同一份数据聚三遍。
--
-- 非分组维度会是 NULL，而 Doris 的 Key 列不允许为空，
-- 所以统一 COALESCE 成字面量 ALL。代价是查询侧必须显式过滤 ——
-- 查整体要带 category1_name='ALL' AND region_name='ALL'，
-- 漏掉任一维度就会把汇总行和明细行一起算，指标翻倍。
INSERT INTO sink_dws_trade_window
SELECT
    CAST(window_start AS DATE)                          AS stat_date,
    'TUMBLE_1M'                                         AS window_type,
    window_start,
    COALESCE(category1_name, 'ALL')                     AS category1_name,
    COALESCE(region_name, 'ALL')                        AS region_name,
    window_end,
    COUNT(DISTINCT order_id)                            AS order_cnt,
    COUNT(DISTINCT user_id)                             AS order_user_cnt,
    CAST(SUM(sku_num) AS BIGINT)                        AS sku_num,
    CAST(SUM(split_amount) AS DECIMAL(18, 2))           AS order_amount,
    CAST(MAX(order_price) AS DECIMAL(16, 2))            AS max_order_price,
    SUM(CAST(is_price_abnormal AS BIGINT))              AS abnormal_price_cnt,
    -- 时效观测：MAX 看最差情况（是否有长尾卡住），AVG 看整体水位。
    -- 只报 AVG 会把偶发的严重延迟平均掉，只报 MAX 又容易被单点噪声带偏，
    -- 两个一起看才判断得出来是链路整体慢了还是个别数据卡住了。
    MAX(lag_seconds)                                    AS max_lag_seconds,
    CAST(AVG(lag_seconds) AS INT)                       AS avg_lag_seconds,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))             AS ingest_time
FROM TABLE(
    TUMBLE(TABLE dwd_trade_order_src, DESCRIPTOR(create_time), INTERVAL '1' MINUTE)
)
GROUP BY
    window_start, window_end,
    GROUPING SETS ((), (category1_name), (region_name));


-- ── 2. 交易域日累计窗口 ─────────────────────────────────────
--
-- CUMULATE：窗口起点固定在零点，终点每分钟前推一格。
-- 看板上「今日累计 GMV」要的正是这个 —— 它是增量维护的，
-- 计算代价与当天已积累的数据量无关，不像每次全表 SUM 那样越跑越慢。
INSERT INTO sink_dws_trade_window
SELECT
    CAST(window_start AS DATE)                          AS stat_date,
    'CUMULATE_1D'                                       AS window_type,
    window_start,
    'ALL'                                               AS category1_name,
    'ALL'                                               AS region_name,
    window_end,
    COUNT(DISTINCT order_id)                            AS order_cnt,
    COUNT(DISTINCT user_id)                             AS order_user_cnt,
    CAST(SUM(sku_num) AS BIGINT)                        AS sku_num,
    CAST(SUM(split_amount) AS DECIMAL(18, 2))           AS order_amount,
    CAST(MAX(order_price) AS DECIMAL(16, 2))            AS max_order_price,
    SUM(CAST(is_price_abnormal AS BIGINT))              AS abnormal_price_cnt,
    MAX(lag_seconds)                                    AS max_lag_seconds,
    CAST(AVG(lag_seconds) AS INT)                       AS avg_lag_seconds,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))             AS ingest_time
FROM TABLE(
    CUMULATE(TABLE dwd_trade_order_src, DESCRIPTOR(create_time),
             INTERVAL '1' MINUTE, INTERVAL '1' DAY)
)
GROUP BY window_start, window_end;


-- ── 3. 支付域分钟窗口 ───────────────────────────────────────
INSERT INTO sink_dws_pay_window
SELECT
    CAST(window_start AS DATE)                          AS stat_date,
    'TUMBLE_1M'                                         AS window_type,
    window_start,
    COALESCE(payment_type, 'ALL')                       AS payment_type,
    window_end,
    COUNT(DISTINCT order_id)                            AS pay_cnt,
    COUNT(DISTINCT user_id)                             AS pay_user_cnt,
    CAST(SUM(payment_amount) AS DECIMAL(18, 2))         AS pay_amount,
    MAX(lag_seconds)                                    AS max_lag_seconds,
    CAST(AVG(lag_seconds) AS INT)                       AS avg_lag_seconds,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))             AS ingest_time
FROM TABLE(
    TUMBLE(TABLE dwd_trade_pay_src, DESCRIPTOR(callback_time), INTERVAL '1' MINUTE)
)
GROUP BY
    window_start, window_end,
    GROUPING SETS ((), (payment_type));


-- ── 4. 支付域日累计窗口 ─────────────────────────────────────
--
-- 支付转化率的分母（下单数）来自交易域累计窗口，分子（支付数）来自这里。
-- 两个分子分母都用 CUMULATE 同一套窗口边界，看板算比值时口径才对得上；
-- 一边用累计窗口、一边用明细现算，跨零点时必然对不上。
INSERT INTO sink_dws_pay_window
SELECT
    CAST(window_start AS DATE)                          AS stat_date,
    'CUMULATE_1D'                                       AS window_type,
    window_start,
    'ALL'                                               AS payment_type,
    window_end,
    COUNT(DISTINCT order_id)                            AS pay_cnt,
    COUNT(DISTINCT user_id)                             AS pay_user_cnt,
    CAST(SUM(payment_amount) AS DECIMAL(18, 2))         AS pay_amount,
    MAX(lag_seconds)                                    AS max_lag_seconds,
    CAST(AVG(lag_seconds) AS INT)                       AS avg_lag_seconds,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))             AS ingest_time
FROM TABLE(
    CUMULATE(TABLE dwd_trade_pay_src, DESCRIPTOR(callback_time),
             INTERVAL '1' MINUTE, INTERVAL '1' DAY)
)
GROUP BY window_start, window_end;


-- ── 5. 迟到数据分流 ─────────────────────────────────────────
--
-- 窗口 TVF 会把超出 watermark 的记录直接丢弃，且不留任何痕迹。
-- 这条 INSERT 和上面的窗口聚合读同一个 source，但不进窗口 ——
-- 直接在流上用 CURRENT_WATERMARK(create_time) 拿到「当前 watermark」，
-- 和记录自己的事件时间比一比，落后的就是注定进不了窗口的那批。
--
-- 两个细节：
--   * CURRENT_WATERMARK 只能作用在 rowtime 属性列上。这里从
--     dwd_trade_order_src 直接查、不经过 join，rowtime 属性才保得住 ——
--     普通 join 之后 create_time 会退化成普通时间戳，函数就用不了了。
--   * 作业刚启动时还没有 watermark，函数返回 NULL，必须先判空，
--     否则第一批数据会因为 NULL 比较全部被当成不迟到（或全部漏判）。
INSERT INTO sink_stream_late_records
SELECT
    CAST(create_time AS DATE)                           AS late_date,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))             AS detect_time,
    CAST(order_detail_id AS STRING)                     AS record_key,
    'dwd_trade_order'                                   AS source_table,
    create_time                                         AS event_time,
    CURRENT_WATERMARK(create_time)                      AS watermark_time,
    CAST(TIMESTAMPDIFF(SECOND, create_time,
                       CURRENT_WATERMARK(create_time)) AS INT) AS lateness_seconds,
    kafka_partition,
    kafka_offset,
    order_id,
    split_amount,
    CAST(0 AS TINYINT)                                  AS is_compensated
FROM dwd_trade_order_src
WHERE CURRENT_WATERMARK(create_time) IS NOT NULL
  AND create_time < CURRENT_WATERMARK(create_time);

END;
