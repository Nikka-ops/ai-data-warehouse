package com.aiwarehouse.flink.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkEnvironment {

    public static StreamExecutionEnvironment create(Properties props) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 配置
        int intervalMs = Integer.parseInt(props.getProperty("checkpoint.interval_ms", "60000"));
        env.enableCheckpointing(intervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(120_000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);

        // RocksDB 状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // 重启策略：指数退避，最多 5 次
        env.setRestartStrategy(
            RestartStrategies.exponentialDelayRestart(
                Time.seconds(1), Time.minutes(10), 2.0, Time.minutes(5), 0
            )
        );

        return env;
    }

    public static Properties loadProps(String configPath) {
        Properties props = new Properties();
        try (var in = FlinkEnvironment.class.getResourceAsStream(configPath)) {
            if (in != null) props.load(in);
        } catch (Exception e) {
            // 使用默认配置
        }
        return props;
    }
}
