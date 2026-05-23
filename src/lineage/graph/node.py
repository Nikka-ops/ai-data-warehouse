# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from enum import Enum

class NodeType(Enum):
    KAFKA_TOPIC = "kafka_topic"
    FLINK_JOB   = "flink_job"
    TABLE       = "table"
    VIEW        = "view"
    COLUMN      = "column"

@dataclass
class LineageNode:
    id: str               # 唯一标识（table_name 或 topic_name）
    name: str
    node_type: NodeType
    database: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
