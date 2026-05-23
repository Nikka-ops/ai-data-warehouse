-- 持续异常检测查询

-- 检测取消率飙升（5分钟窗口）
SELECT
    TUMBLE_START(event_time, INTERVAL '5' MINUTE)                    AS window_start,
    category,
    COUNT(*) FILTER (WHERE status = 'cancelled') * 100.0 / COUNT(*)  AS cancel_rate,
    COUNT(*)                                                          AS total_orders
FROM orders_source
GROUP BY category, TUMBLE(event_time, INTERVAL '5' MINUTE)
HAVING cancel_rate > 15.0;

-- 检测 GMV 断崖（对比前一个窗口）
SELECT
    curr.window_start,
    curr.total_gmv                              AS curr_gmv,
    prev.total_gmv                              AS prev_gmv,
    (curr.total_gmv - prev.total_gmv) / prev.total_gmv * 100 AS gmv_change_pct
FROM minute_stats_sink curr
LEFT JOIN minute_stats_sink prev
    ON curr.window_start = prev.window_start + INTERVAL '1' MINUTE
WHERE prev.total_gmv > 0
  AND curr.total_gmv < prev.total_gmv * 0.5;
