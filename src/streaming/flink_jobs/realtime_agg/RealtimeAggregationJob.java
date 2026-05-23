package com.aiwarehouse.flink.realtime_agg;

import com.aiwarehouse.flink.common.ConfigConstants;
import com.aiwarehouse.flink.common.FlinkEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时聚合作业：使用 Flink SQL 实现，比 DataStream API 更易维护
 */
public class RealtimeAggregationJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvironment.create(
            FlinkEnvironment.loadProps("/checkpoint_config.yaml"));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 定义 Kafka Source 表
        tEnv.executeSql(String.format("""
            CREATE TABLE orders_kafka (
                order_id      STRING,
                customer_id   STRING,
                seller_id     STRING,
                category      STRING,
                price         DOUBLE,
                quantity      INT,
                event_time    TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '%s',
                'properties.bootstrap.servers' = '%s',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            )
            """,
            ConfigConstants.TOPIC_ORDERS,
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        ));

        // 1 分钟滚动窗口聚合
        tEnv.executeSql("""
            INSERT INTO clickhouse_minute_stats
            SELECT
                TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
                category,
                COUNT(*)           AS order_cnt,
                SUM(price*quantity) AS total_gmv,
                AVG(price)         AS avg_price
            FROM orders_kafka
            GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), category
            """);

        env.execute("RealtimeAggregationJob");
    }
}
