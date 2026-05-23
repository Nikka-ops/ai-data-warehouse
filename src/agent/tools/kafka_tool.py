# -*- coding: utf-8 -*-
"""Kafka 管理工具"""
from langchain_core.tools import tool


@tool
def get_kafka_lag(group_id: str = "flink-feature-compute") -> str:
    """查询指定消费组的 Kafka lag"""
    try:
        from kafka.admin import KafkaAdminClient
        from src.common.config import cfg
        admin = KafkaAdminClient(bootstrap_servers=cfg.kafka_bootstrap)
        offsets = admin.list_consumer_group_offsets(group_id)
        total_lag = sum(v.offset for v in offsets.values()) if offsets else 0
        return f"消费组 {group_id} 当前总 Lag: {total_lag}"
    except Exception as e:
        return f"查询 Kafka lag 失败: {e}"


@tool
def list_kafka_topics() -> str:
    """列出所有 Kafka Topics 及分区数"""
    try:
        from kafka.admin import KafkaAdminClient
        from src.common.config import cfg
        admin = KafkaAdminClient(bootstrap_servers=cfg.kafka_bootstrap)
        topics = admin.list_topics()
        return "\n".join(sorted(t for t in topics if not t.startswith('_')))
    except Exception as e:
        return f"获取 Topics 失败: {e}"
