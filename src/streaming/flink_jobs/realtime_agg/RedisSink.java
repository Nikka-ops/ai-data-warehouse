package com.aiwarehouse.flink.realtime_agg;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 实时特征写 Redis（用于在线低延迟特征服务）
 */
public class RedisSink extends RichSinkFunction<UserFeature> {

    private JedisPool pool;

    @Override
    public void open(org.apache.flink.configuration.Configuration params) {
        String host = System.getenv().getOrDefault("REDIS_HOST", "redis");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        pool = new JedisPool(config, host, port);
    }

    @Override
    public void invoke(UserFeature feature, Context ctx) throws Exception {
        try (Jedis jedis = pool.getResource()) {
            String key = "feat:user:" + feature.getUserId();
            jedis.hset(key, feature.toMap());
            jedis.expire(key, 3600);  // TTL 1 小时
        }
    }

    @Override
    public void close() {
        if (pool != null) pool.close();
    }
}
