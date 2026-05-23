# -*- coding: utf-8 -*-
"""
生产者抽象基类，定义统一的消息发送接口
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.common.models import OrderEvent


class BaseProducer(ABC):
    """所有生产者实现必须继承此基类"""

    @abstractmethod
    def produce(self, topic: str, key: str, value: dict) -> None:
        """发送单条消息到指定 topic"""
        ...

    @abstractmethod
    def flush(self) -> None:
        """强制将缓冲区消息全部发送"""
        ...

    @abstractmethod
    def close(self) -> None:
        """关闭生产者，释放连接资源"""
        ...

    def produce_order(self, event: OrderEvent) -> None:
        """发送订单事件（便捷方法，自动选择 topic 和 key）"""
        self.produce(
            topic="orders_stream",
            key=event.order_id,
            value=event.model_dump(),  # Pydantic v2 序列化为 dict
        )
