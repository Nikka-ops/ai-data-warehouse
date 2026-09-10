-- ============================================================
-- DWD 层作业：CDC → 清洗打宽 → Kafka
--
--   MySQL binlog ──> 维度退化 ──> dwd_trade_order  (Kafka)
--                              ├─> dwd_trade_pay    (Kafka)
--                              ├─> dwd_trade_refund (Kafka)
--                              └─> dwd.order_snapshot (Doris)
--
-- 提交：sql-client.sh -i 00_init.sql -f 10_dwd_job.sql
-- ============================================================

SET 'pipeline.name' = 'rtdw-dwd';

EXECUTE STATEMENT SET
BEGIN

-- ── 1. 下单事务事实：明细 + 维度退化 ────────────────────────
--
-- 维度退化（degenerate dimension）是数仓建模的标准动作：
-- 把品类名、省份名这些低基数属性直接冗余进事实表，
-- 下游聚合时就不必再 join 维表。流处理里这一点尤其重要 ——
-- 每个 DWS 作业都自己去 join 一次维表，会把维表打成热点。
--
-- 用 FOR SYSTEM_TIME AS OF proc_time 的处理时间语义：
-- 按数据流过来那一刻的维表快照关联，符合维度缓慢变化的预期。
INSERT INTO dwd_trade_order
SELECT
    od.id                                       AS order_detail_id,
    od.order_id,
    oi.user_id,
    od.sku_id,
    sku.sku_name,
    sku.category3_id,
    cat.name                                    AS category3_name,
    cat.category1_name,
    oi.province_id,
    prov.name                                   AS province_name,
    prov.region_name,
    od.sku_num,
    od.order_price,
    od.split_total_amount                       AS split_amount,
    -- 成交价显著偏离商品标价，通常意味着配置错误或薅羊毛
    CASE WHEN sku.price > 0
              AND (od.order_price > sku.price * 1.5
                   OR od.order_price < sku.price * 0.3)
         THEN CAST(1 AS TINYINT) ELSE CAST(0 AS TINYINT) END AS is_price_abnormal,
    od.create_time,
    -- 端到端延迟：业务库写下这条明细，到 Flink 把它加工完，中间过了多久。
    -- 这是「实时到底有多实时」唯一拿得出来的证据 —— 下游对它做 MAX/AVG
    -- 就是链路时效指标，超阈值就是质检器要报的 LATENCY 告警。
    --
    -- 用 LOCALTIMESTAMP 而不是 CURRENT_TIMESTAMP：后者在 Flink 里是
    -- TIMESTAMP_LTZ，和 TIMESTAMP(3) 的 create_time 做 TIMESTAMPDIFF 会报
    -- "TIMESTAMP_LTZ only supports diff between the same type"。
    -- LOCALTIMESTAMP 就是 TIMESTAMP(3)，两边类型一致。
    CAST(TIMESTAMPDIFF(SECOND, od.create_time, LOCALTIMESTAMP) AS INT) AS lag_seconds,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))     AS ingest_time
FROM ods_order_detail AS od
-- 订单主表走 lookup 而不是常规 join：
-- 常规 join 一个 CDC changelog 会让整条流带上撤回语义，
-- 普通 Kafka sink 消费不了；而这里要补的 user_id / province_id
-- 下单之后就不再变，按处理时间点查一次即可。理由详见 00_init.sql。
LEFT JOIN dim_order_info FOR SYSTEM_TIME AS OF od.proc_time AS oi
       ON od.order_id = oi.id
LEFT JOIN dim_sku FOR SYSTEM_TIME AS OF od.proc_time AS sku
       ON od.sku_id = sku.id
LEFT JOIN dim_category FOR SYSTEM_TIME AS OF od.proc_time AS cat
       ON sku.category3_id = cat.id
LEFT JOIN dim_province FOR SYSTEM_TIME AS OF od.proc_time AS prov
       ON oi.province_id = prov.id;


-- ── 2. 支付事务事实 ─────────────────────────────────────────
INSERT INTO dwd_trade_pay
SELECT
    p.id                                        AS payment_id,
    p.order_id,
    p.user_id,
    p.payment_type,
    p.payment_amount,
    p.callback_time,
    CAST(TIMESTAMPDIFF(SECOND, p.callback_time, LOCALTIMESTAMP) AS INT) AS lag_seconds,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))     AS ingest_time
FROM ods_payment_info AS p
WHERE p.payment_status = 'SUCCESS';


-- ── 3. 退款事务事实 ─────────────────────────────────────────
INSERT INTO dwd_trade_refund
SELECT
    r.id                                        AS refund_id,
    r.order_id,
    r.user_id,
    r.refund_type,
    r.refund_amount,
    r.refund_reason,
    r.create_time,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))     AS ingest_time
FROM ods_refund_info AS r
WHERE r.refund_status = 'SUCCESS';


-- ── 4. 订单累积快照 → Doris ─────────────────────────────────
--
-- 这条链路承载的是订单的「当前状态」，而不是事件。
-- 数据源是 order_info 的 changelog：同一个 order_id 会随着
-- CREATED → PAID → SHIPPED → DELIVERED 被写入多次。
--
-- 之所以能直接 upsert 进 Doris 而不需要额外处理撤回：
-- Doris 侧该表是 Unique Key 模型，按主键覆盖；
-- 且配了 update_time 作为 sequence 列，即使 binlog 事件乱序到达，
-- update_time 小的旧状态也不会把新状态盖回去。
--
-- pay_lag_seconds 记录下单到支付的时长，是运营关心的转化时效指标，
-- 也顺带体现了累积快照相比事务事实表的价值：跨状态的时间差
-- 只有在快照表里才算得出来。
INSERT INTO sink_dwd_order_snapshot
SELECT
    CAST(oi.create_time AS DATE)                AS create_date,
    oi.id                                       AS order_id,
    oi.update_time,
    oi.user_id,
    oi.province_id,
    prov.name                                   AS province_name,
    prov.region_name,
    oi.order_status,
    oi.total_amount,
    oi.activity_reduce,
    oi.coupon_reduce,
    oi.freight_amount,
    oi.create_time,
    oi.payment_time,
    oi.ship_time,
    oi.receive_time,
    CASE WHEN oi.payment_time IS NOT NULL
         THEN CAST(TIMESTAMPDIFF(SECOND, oi.create_time, oi.payment_time) AS INT)
         ELSE CAST(NULL AS INT) END             AS pay_lag_seconds,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))     AS ingest_time
FROM ods_order_info AS oi
LEFT JOIN dim_province FOR SYSTEM_TIME AS OF oi.proc_time AS prov
       ON oi.province_id = prov.id;

END;
