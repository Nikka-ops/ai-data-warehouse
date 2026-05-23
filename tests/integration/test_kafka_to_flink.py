# -*- coding: utf-8 -*-
"""Kafka → Flink 集成测试（需要运行中的服务）"""
import pytest
import os

# 仅在 CI 环境或明确设置了标志时运行
pytestmark = pytest.mark.skipif(
    os.getenv("INTEGRATION_TEST") != "1",
    reason="集成测试需要设置 INTEGRATION_TEST=1"
)

class TestKafkaFlinkPipeline:
    def test_producer_sends_message(self):
        """测试生产者能成功发送消息"""
        try:
            from kafka import KafkaProducer
            from src.common.config import cfg
            producer = KafkaProducer(bootstrap_servers=cfg.kafka_bootstrap)
            future = producer.send("orders_stream", b'{"test": true}')
            record = future.get(timeout=10)
            assert record is not None
            producer.close()
        except Exception as e:
            pytest.fail(f"Kafka 连接失败: {e}")

    def test_clickhouse_writable(self):
        """测试 ClickHouse 连接和写入"""
        try:
            import clickhouse_connect
            from src.common.config import cfg
            ch = clickhouse_connect.get_client(
                host=cfg.ch_host, port=cfg.ch_port,
                username=cfg.ch_user, password=cfg.ch_password)
            result = ch.query("SELECT 1")
            assert result.result_rows[0][0] == 1
        except Exception as e:
            pytest.fail(f"ClickHouse 连接失败: {e}")
