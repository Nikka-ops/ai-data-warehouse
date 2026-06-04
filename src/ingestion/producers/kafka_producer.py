# -*- coding: utf-8 -*-
"""
Kafka 生产者实现，基于 kafka-python，JSON 序列化，带重试
"""

from __future__ import annotations

import json

try:
    from kafka import KafkaProducer as _KafkaProducer  # type: ignore[attr-defined]
    _KAFKA_AVAILABLE = True
except ImportError:
    _KafkaProducer = None  # type: ignore[assignment]
    _KAFKA_AVAILABLE = False  # 缺少依赖时不崩溃

from src.common.config import cfg
from src.common.utils import get_logger
from src.ingestion.producers.base_producer import BaseProducer

log = get_logger("ingestion.kafka_producer")


class KafkaProducer(BaseProducer):
    """真实 Kafka 生产者，连接 cfg.kafka_bootstrap，JSON 序列化消息"""

    def __init__(self) -> None:
        if not _KAFKA_AVAILABLE:
            raise ImportError("kafka-python 未安装，请执行 pip install kafka-python")
        log.info("连接 Kafka broker：%s", cfg.kafka_bootstrap)
        self._producer = _KafkaProducer(
            bootstrap_servers=cfg.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",                 # 等待所有副本确认，保证不丢消息
            retries=3,                  # 网络抖动时自动重试
            max_block_ms=10000,         # 发送阻塞最大 10 秒
            batch_size=32768,           # 32KB 批次，平衡延迟与吞吐
            linger_ms=5,                # 最多等 5ms 凑批
            compression_type="gzip",   # gzip 压缩减少网络带宽
        )

    def produce(self, topic: str, key: str, value: dict,
               sync: bool = False, timeout: float = 10.0) -> None:
        """发送消息。

        sync=True 时阻塞等待 broker 确认，失败则抛出 KafkaError（适合需要强可靠性的场景）。
        sync=False（默认）时异步发送，失败仅记录日志——调用方无法感知投递结果。
        """
        future = self._producer.send(topic, key=key, value=value)
        if sync:
            future.get(timeout=timeout)  # 阻塞确认，失败抛出异常
        else:
            future.add_errback(lambda exc: log.error("Kafka 发送失败 topic=%s：%s", topic, exc))

    def flush(self) -> None:
        """等待所有 in-flight 消息发送完成"""
        self._producer.flush()
        log.debug("Kafka producer flush 完成")

    def close(self) -> None:
        """关闭连接，先 flush 再关闭"""
        self._producer.flush()
        self._producer.close()
        log.info("Kafka producer 已关闭")
