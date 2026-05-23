# -*- coding: utf-8 -*-
"""
核心数据模型定义，全项目共享
"""

from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class OrderEvent(BaseModel):
    """电商订单事件，对应 Kafka orders_stream 消息结构"""
    order_id: str
    customer_id: str
    seller_id: str
    product_id: str
    category: str
    price: float
    quantity: int
    event_time: datetime
    state: str  # 巴西州代码，如 SP/RJ/MG


class AlertEvent(BaseModel):
    """系统/业务告警事件，用于告警路由和降噪"""
    alert_id: str = ""                  # 唯一告警 ID，为空时自动生成
    source: str                          # 告警来源服务名
    category: str                        # 分类：DATA_QUALITY | SYSTEM | BUSINESS
    severity: str = "P3"                 # 优先级：P1（最高）~ P4（最低）
    title: str                           # 告警标题
    detail: str = ""                     # 详细描述
    metric_name: str = ""                # 触发告警的指标名
    current_value: float = 0.0           # 指标当前值
    threshold_value: float = 0.0         # 触发阈值
    affected_tables: list[str] = []      # 受影响的表列表
    fired_at: datetime = Field(default_factory=datetime.now)  # 告警触发时间
    fingerprint: str = ""                # 去重指纹，空时由路由器计算


class QueryResult(BaseModel):
    """NL2SQL 查询结果，携带性能指标和置信度"""
    sql: str                             # 执行的 SQL 语句
    data: list[dict]                     # 查询结果行列表
    row_count: int                       # 实际返回行数
    elapsed_ms: float                    # 查询耗时（毫秒）
    repair_attempts: int = 0             # SQL 自动修复尝试次数
    insight_confidence: float = 1.0      # AI 洞察置信度 [0, 1]


class FeatureVector(BaseModel):
    """实体特征向量，缓存于 Redis 在线特征存储"""
    entity_id: str                       # 实体唯一标识（用户/卖家/品类 ID）
    entity_type: str                     # 实体类型：user | seller | category
    features: dict[str, float | int | str]  # 特征名 → 特征值
    computed_at: datetime                # 特征计算时间戳
    ttl_seconds: int = 3600              # 缓存过期时间（秒），默认 1 小时
