# -*- coding: utf-8 -*-
"""
内存队列 MockProducer（测试用，不真正连接 Kafka）
以及基于真实巴西电商分布的模拟数据生成器 BrazilianEcommerceSimulator
数据分布逻辑迁移自 kafka/producer.py
"""

from __future__ import annotations

import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta
from typing import Deque

from src.common.models import OrderEvent
from src.common.utils import get_logger
from src.ingestion.producers.base_producer import BaseProducer

log = get_logger("ingestion.mock_producer")

# ── 地理分布（真实巴西电商权重）────────────────────────────────────
_STATES = {
    "SP": 0.42, "RJ": 0.13, "MG": 0.11, "RS": 0.06,
    "PR": 0.05, "SC": 0.04, "BA": 0.04, "GO": 0.03,
    "DF": 0.03, "PE": 0.02, "CE": 0.02, "AM": 0.01,
    "ES": 0.01, "MT": 0.01, "MS": 0.01, "PA": 0.01,
}

# ── 品类配置（权重 + 价格区间）──────────────────────────────────────
_CATEGORIES = {
    "beleza_saude":           {"weight": 0.15, "price": (20,  300)},
    "relogios_presentes":     {"weight": 0.10, "price": (50,  800)},
    "cama_mesa_banho":        {"weight": 0.11, "price": (30,  500)},
    "esporte_lazer":          {"weight": 0.10, "price": (40,  400)},
    "informatica_acessorios": {"weight": 0.09, "price": (30,  600)},
    "moveis_decoracao":       {"weight": 0.08, "price": (80,  1500)},
    "utilidades_domesticas":  {"weight": 0.07, "price": (15,  200)},
    "automotivo":             {"weight": 0.06, "price": (30,  500)},
    "brinquedos":             {"weight": 0.05, "price": (20,  300)},
    "telefonia":              {"weight": 0.07, "price": (100, 3000)},
    "eletronicos":            {"weight": 0.07, "price": (80,  2000)},
    "ferramentas_jardim":     {"weight": 0.05, "price": (20,  400)},
}

# Pareto 热门/长尾商品池（热门 20% 贡献 80% 订单）
_HOT_PRODUCTS  = [f"P{random.randint(100000, 400000)}" for _ in range(200)]
_LONG_PRODUCTS = [f"P{random.randint(400001, 999999)}" for _ in range(800)]


def _weighted_choice(d: dict) -> str:
    """按权重随机选择 key"""
    return random.choices(list(d.keys()), weights=list(d.values()), k=1)[0]


class MockProducer(BaseProducer):
    """内存队列生产者，用于单元测试和本地调试，不连接 Kafka"""

    def __init__(self, maxlen: int = 10000) -> None:
        self._queues: dict[str, Deque[dict]] = {}  # topic → deque
        self._maxlen = maxlen                       # 每个 topic 最大缓存条数

    def produce(self, topic: str, key: str, value: dict) -> None:
        """将消息追加到内存队列"""
        if topic not in self._queues:
            self._queues[topic] = deque(maxlen=self._maxlen)
        self._queues[topic].append({"key": key, "value": value})

    def flush(self) -> None:
        """内存队列无需 flush，空实现"""
        pass

    def close(self) -> None:
        """清空所有队列"""
        self._queues.clear()
        log.debug("MockProducer 已关闭，队列已清空")

    def get_messages(self, topic: str) -> list[dict]:
        """获取指定 topic 的所有消息（测试断言用）"""
        return list(self._queues.get(topic, []))

    def clear(self, topic: str | None = None) -> None:
        """清空队列；topic 为 None 时清空全部"""
        if topic:
            self._queues.pop(topic, None)
        else:
            self._queues.clear()


class BrazilianEcommerceSimulator:
    """
    巴西电商模拟数据生成器
    数据分布基于真实 Olist 数据集统计，包括地理权重、品类权重、Pareto 热门商品
    """

    def generate_order(self) -> OrderEvent:
        """生成一条符合真实分布的模拟订单事件"""
        state   = _weighted_choice(_STATES)
        cat     = _weighted_choice({k: v["weight"] for k, v in _CATEGORIES.items()})
        price   = round(random.uniform(*_CATEGORIES[cat]["price"]), 2)  # type: ignore[misc]
        # 5% 概率产生轻微时间延迟（模拟网络抖动）
        delay   = timedelta(minutes=random.randint(0, 5)) if random.random() < 0.05 else timedelta(0)
        product = random.choice(_HOT_PRODUCTS if random.random() < 0.8 else _LONG_PRODUCTS)

        return OrderEvent(
            order_id=str(uuid.uuid4()),
            customer_id=f"C{random.randint(10000, 99999)}",
            seller_id=f"S{random.randint(1000, 9999)}",
            product_id=product,
            category=cat,
            price=price,
            quantity=random.choices([1, 2, 3], weights=[0.75, 0.18, 0.07])[0],
            event_time=datetime.now() - delay,
            state=state,
        )

    def run(
        self,
        producer: BaseProducer,
        rate_per_second: int = 20,
        duration_seconds: float | None = None,
    ) -> None:
        """
        持续生成订单并通过 producer 发送
        rate_per_second: 每秒生成条数
        duration_seconds: 运行秒数，None 表示永久运行直到 KeyboardInterrupt
        """
        interval  = 1.0 / max(rate_per_second, 1)
        deadline  = (time.time() + duration_seconds) if duration_seconds else None
        count     = 0
        log.info("模拟器启动，速率 %d/s", rate_per_second)
        try:
            while True:
                if deadline and time.time() >= deadline:
                    break
                t0 = time.time()
                order = self.generate_order()
                producer.produce_order(order)
                count += 1
                if count % 1000 == 0:
                    log.info("已生成 %d 条模拟订单", count)
                elapsed = time.time() - t0
                sleep_t = max(0.0, interval - elapsed)
                if sleep_t > 0:
                    time.sleep(sleep_t)
        except KeyboardInterrupt:
            pass
        finally:
            producer.flush()
            log.info("模拟器停止，共生成 %d 条订单", count)
