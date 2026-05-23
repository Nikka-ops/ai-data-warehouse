package com.aiwarehouse.flink.feature_compute;

import com.aiwarehouse.flink.common.ConfigConstants;
import com.aiwarehouse.flink.common.FlinkEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

/**
 * 特征计算主作业：从 Kafka 消费订单事件，计算用户/商品特征，写入 ClickHouse + Redis
 */
public class FeatureComputeJob {

    public static void main(String[] args) throws Exception {
        Properties props = FlinkEnvironment.loadProps("/checkpoint_config.yaml");
        StreamExecutionEnvironment env = FlinkEnvironment.create(props);

        // Kafka Source
        KafkaSource<OrderEvent> source = KafkaSource.<OrderEvent>builder()
            .setBootstrapServers(System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
            .setTopics(ConfigConstants.TOPIC_ORDERS)
            .setGroupId("flink-feature-compute")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new UserEventDeserializer())
            .build();

        // 事件时间水印：允许 5 秒乱序
        DataStream<OrderEvent> orderStream = env
            .fromSource(source, WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.getEventTimeMs()), "Kafka Order Source");

        // 1 分钟滚动窗口聚合
        DataStream<MinuteStats> minuteStats = orderStream
            .keyBy(OrderEvent::getCategory)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new WindowAggregator());

        // 写 ClickHouse
        minuteStats.addSink(new ClickHouseSink(ConfigConstants.TABLE_MINUTE_STATS));

        // 用户特征写 Redis
        orderStream
            .keyBy(OrderEvent::getCustomerId)
            .process(new UserFeatureProcessor())
            .addSink(new RedisSink());

        env.execute("FeatureComputeJob");
    }
}
