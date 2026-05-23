-- 用户维度特征计算（持续查询）

INSERT INTO user_feature_sink
SELECT
    customer_id,
    COUNT(*)                                          AS order_count_1h,
    SUM(price * quantity)                             AS gmv_1h,
    AVG(price)                                        AS avg_price_1h,
    COUNT(DISTINCT category)                          AS category_cnt_1h,
    TUMBLE_START(event_time, INTERVAL '1' HOUR)       AS feature_time
FROM orders_source
WHERE event_time >= NOW() - INTERVAL '1' HOUR
GROUP BY
    customer_id,
    TUMBLE(event_time, INTERVAL '1' HOUR);
