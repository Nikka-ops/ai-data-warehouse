package com.aiwarehouse.flink.common;

public final class ConfigConstants {
    private ConfigConstants() {}

    // Kafka Topics
    public static final String TOPIC_ORDERS    = "orders_stream";
    public static final String TOPIC_PAYMENTS  = "payments_stream";
    public static final String TOPIC_STATS     = "flink.minute_stats";
    public static final String TOPIC_DWD       = "flink.realtime_dwd";
    public static final String TOPIC_ALERTS    = "flink.alerts";

    // ClickHouse Tables
    public static final String TABLE_MINUTE_STATS = "dws.realtime_minute_stats";
    public static final String TABLE_KAPPA_AGG    = "dws.kappa_hourly_agg";
    public static final String TABLE_REALTIME_DWD = "dwd.realtime_order_detail";

    // Window Sizes
    public static final long WINDOW_1MIN_MS  = 60_000L;
    public static final long WINDOW_5MIN_MS  = 300_000L;
    public static final long WINDOW_1HOUR_MS = 3_600_000L;

    // State TTL (seconds)
    public static final long STATE_TTL_ORDER   = 86_400L;  // 1 day
    public static final long STATE_TTL_SESSION = 1_800L;   // 30 min
}
