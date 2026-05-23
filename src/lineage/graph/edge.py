# -*- coding: utf-8 -*-
from dataclasses import dataclass
from enum import Enum

class EdgeType(Enum):
    READS_FROM   = "reads_from"    # Flink 从 Kafka/表读取
    WRITES_TO    = "writes_to"     # 写入表/Topic
    DERIVED_FROM = "derived_from"  # 物化视图/聚合
    JOIN_WITH    = "join_with"     # JOIN 关系

@dataclass
class LineageEdge:
    source: str       # 源节点 id
    target: str       # 目标节点 id
    edge_type: EdgeType
    transform: str = ""    # 变换描述
    latency_ms: int = 0    # 处理延迟
