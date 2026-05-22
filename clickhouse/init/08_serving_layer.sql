-- ============================================================
-- Kappa 架构服务层：单一流处理路径，无水位线切割
-- Kafka → Flink(实时 + 回放) → ClickHouse(服务层)
-- ============================================================

-- ── 日级聚合视图（供 Superset 历史趋势图使用）────────────────
CREATE VIEW IF NOT EXISTS dws.kappa_daily_trend AS
SELECT
    toDate(hour_start)          AS stat_date,
    sum(order_cnt)              AS order_cnt,
    round(sum(total_gmv), 2)    AS total_gmv,
    round(
        sum(total_gmv) / nullIf(sum(order_cnt), 0), 2
    )                           AS avg_price,
    sum(cancel_cnt)             AS cancel_cnt,
    sum(unique_customers)       AS unique_customers
FROM dws.kappa_hourly_agg
GROUP BY stat_date
ORDER BY stat_date;

-- ── 品类维度视图（Kappa 统一版，历史回放 + 实时互补）────────
CREATE VIEW IF NOT EXISTS dws.kappa_category_stats AS
SELECT
    toDate(hour_start)            AS stat_date,
    product_category,
    sum(order_cnt)                AS order_cnt,
    round(sum(total_gmv), 2)      AS total_gmv,
    round(avg(avg_price), 2)      AS avg_price,
    sum(cancel_cnt)               AS cancel_cnt
FROM dws.kappa_hourly_agg
WHERE product_category != ''
GROUP BY stat_date, product_category;

-- ── 回放健康状态视图（监控最新回放任务）─────────────────────
CREATE VIEW IF NOT EXISTS stream.kappa_replay_status AS
SELECT
    job_id,
    job_name,
    triggered_by,
    from_offset,
    start_time,
    end_time,
    records_processed,
    status,
    dateDiff('second', start_time, coalesce(end_time, now())) AS elapsed_seconds,
    round(records_processed / nullIf(
        dateDiff('second', start_time, coalesce(end_time, now())), 0
    ), 0)                              AS records_per_second,
    error_msg
FROM stream.kappa_replay_jobs
ORDER BY start_time DESC
LIMIT 20;
