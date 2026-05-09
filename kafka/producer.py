# -*- coding: utf-8 -*-
"""
实时订单模拟生产者
模拟真实电商平台的订单和支付消息，持续写入 Kafka

运行：python kafka/producer.py
可调参数：--rate（每秒消息数）--burst（突发模式）
"""

import json
import random
import time
import uuid
import argparse
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── 模拟数据配置 ──────────────────────────────────────────────
KAFKA_BOOTSTRAP = 'localhost:9092'
ORDERS_TOPIC    = 'orders_stream'
PAYMENTS_TOPIC  = 'payments_stream'

# 巴西各州权重（SP 占比最高，和真实数据一致）
STATES = {
    'SP': 0.42, 'RJ': 0.13, 'MG': 0.11, 'RS': 0.06,
    'PR': 0.05, 'SC': 0.04, 'BA': 0.04, 'GO': 0.03,
    'DF': 0.03, 'PE': 0.02, 'CE': 0.02, 'AM': 0.01,
    'ES': 0.01, 'MT': 0.01, 'MS': 0.01, 'PA': 0.01,
}

# 商品品类和价格分布
CATEGORIES = {
    'Beleza_Saude':         {'weight': 0.15, 'price_range': (20, 300)},
    'Relogios_Presentes':   {'weight': 0.12, 'price_range': (50, 800)},
    'Cama_Mesa_Banho':      {'weight': 0.11, 'price_range': (30, 500)},
    'Esporte_Lazer':        {'weight': 0.10, 'price_range': (40, 400)},
    'Informatica_Acessorios':{'weight': 0.09,'price_range': (30, 600)},
    'Moveis_Decoracao':     {'weight': 0.08, 'price_range': (50, 1000)},
    'Utilidades_Domesticas':{'weight': 0.07, 'price_range': (15, 200)},
    'Automotivo':           {'weight': 0.06, 'price_range': (30, 500)},
    'Brinquedos':           {'weight': 0.05, 'price_range': (20, 300)},
    'Telefonia':            {'weight': 0.07, 'price_range': (100, 2000)},
    'Eletronicos':          {'weight': 0.05, 'price_range': (50, 1500)},
    'Ferramentas_Jardim':   {'weight': 0.05, 'price_range': (20, 400)},
}

ORDER_STATUSES = ['created', 'approved', 'processing', 'shipped', 'delivered', 'canceled']
PAYMENT_TYPES  = ['credit_card', 'boleto', 'voucher', 'debit_card']

# 巴西城市
CITIES = {
    'SP': ['Sao Paulo', 'Campinas', 'Santos', 'Ribeirao Preto'],
    'RJ': ['Rio de Janeiro', 'Niteroi', 'Petropolis'],
    'MG': ['Belo Horizonte', 'Uberlandia', 'Contagem'],
    'RS': ['Porto Alegre', 'Caxias do Sul', 'Pelotas'],
}


def weighted_choice(weights_dict: dict) -> str:
    """按权重随机选择"""
    items = list(weights_dict.keys())
    weights = list(weights_dict.values())
    return random.choices(items, weights=weights, k=1)[0]


def generate_order_message() -> dict:
    """生成一条模拟订单消息"""
    state    = weighted_choice(STATES)
    cities   = CITIES.get(state, ['Unknown'])
    city     = random.choice(cities)
    category = weighted_choice({k: v['weight'] for k, v in CATEGORIES.items()})
    price_range = CATEGORIES[category]['price_range']
    price    = round(random.uniform(*price_range), 2)

    # 模拟延迟消息（5%概率产生5分钟前的消息，测试乱序处理）
    delay = timedelta(minutes=random.randint(0, 5)) if random.random() < 0.05 else timedelta(0)
    event_time = (datetime.now() - delay).strftime('%Y-%m-%d %H:%M:%S')

    return {
        'order_id':         str(uuid.uuid4()),
        'customer_id':      f"C{random.randint(10000, 99999)}",
        'product_id':       f"P{random.randint(100000, 999999)}",
        'product_category': category,
        'seller_id':        f"S{random.randint(1000, 9999)}",
        'price':            price,
        'freight_value':    round(random.uniform(5, 50), 2),
        'order_status':     random.choices(
            ORDER_STATUSES,
            weights=[0.05, 0.10, 0.10, 0.15, 0.55, 0.05]
        )[0],
        'state':       state,
        'city':        city,
        'event_time':  event_time,
        'msg_version': '1.0',
    }


def generate_payment_message(order_id: str) -> dict:
    """生成对应的支付消息"""
    payment_type = random.choices(
        PAYMENT_TYPES,
        weights=[0.75, 0.15, 0.06, 0.04]
    )[0]

    return {
        'payment_id':    str(uuid.uuid4()),
        'order_id':      order_id,
        'payment_type':  payment_type,
        'payment_value': round(random.uniform(10, 500), 2),
        'installments':  random.choices([1, 2, 3, 6, 12], weights=[0.4, 0.2, 0.2, 0.1, 0.1])[0],
        'event_time':    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'msg_version':   '1.0',
    }


class OrderProducer:
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            # 生产级配置
            acks='all',              # 等待所有副本确认
            retries=3,               # 失败重试3次
            batch_size=16384,        # 批量发送，提升吞吐
            linger_ms=10,            # 最多等10ms攒批次
            compression_type='gzip', # 压缩减少网络传输
        )
        self.sent_orders    = 0
        self.sent_payments  = 0
        self.failed_count   = 0
        self.start_time     = time.time()

    def send_order(self, order: dict):
        """发送订单消息"""
        future = self.producer.send(ORDERS_TOPIC, value=order)
        future.add_callback(self._on_success)
        future.add_errback(self._on_error)

    def send_payment(self, payment: dict):
        """发送支付消息（80%的订单有支付记录）"""
        future = self.producer.send(PAYMENTS_TOPIC, value=payment)
        future.add_callback(self._on_success)
        future.add_errback(self._on_error)

    def _on_success(self, record_metadata):
        topic = record_metadata.topic
        if topic == ORDERS_TOPIC:
            self.sent_orders += 1
        else:
            self.sent_payments += 1

    def _on_error(self, exc):
        self.failed_count += 1
        print(f"[ERROR] 发送失败：{exc}")

    def print_stats(self):
        elapsed = time.time() - self.start_time
        rate_o  = self.sent_orders   / elapsed if elapsed > 0 else 0
        rate_p  = self.sent_payments / elapsed if elapsed > 0 else 0
        print(
            f"\r[{datetime.now().strftime('%H:%M:%S')}] "
            f"订单: {self.sent_orders:,}条 ({rate_o:.1f}/s)  "
            f"支付: {self.sent_payments:,}条 ({rate_p:.1f}/s)  "
            f"失败: {self.failed_count}",
            end='', flush=True
        )

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()


def run_normal_mode(rate: int = 5):
    """正常模式：稳定速率发送"""
    print(f"[正常模式] 速率：{rate} 条/秒，按 Ctrl+C 停止")
    producer = OrderProducer()
    interval = 1.0 / rate

    try:
        while True:
            order = generate_order_message()
            producer.send_order(order)

            # 80% 概率同时产生支付记录
            if random.random() < 0.8:
                payment = generate_payment_message(order['order_id'])
                producer.send_payment(payment)

            producer.print_stats()
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n停止生产者...")
    finally:
        producer.close()
        print(f"共发送：{producer.sent_orders} 订单，{producer.sent_payments} 支付")


def run_burst_mode(burst_size: int = 1000, interval: int = 60):
    """突发模式：每隔N秒发送一批（模拟促销活动）"""
    print(f"[突发模式] 每 {interval} 秒发送 {burst_size} 条，模拟促销活动")
    producer = OrderProducer()

    try:
        while True:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 开始发送 {burst_size} 条消息...")
            for i in range(burst_size):
                order = generate_order_message()
                producer.send_order(order)
                if random.random() < 0.8:
                    producer.send_payment(generate_payment_message(order['order_id']))
                if i % 100 == 0:
                    producer.print_stats()

            producer.flush()
            print(f"\n本批发送完成，等待 {interval} 秒...")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n停止生产者...")
    finally:
        producer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AI 数仓 Kafka 订单生产者')
    parser.add_argument('--mode',  choices=['normal', 'burst'], default='normal')
    parser.add_argument('--rate',  type=int, default=5,    help='正常模式：每秒消息数')
    parser.add_argument('--burst', type=int, default=1000, help='突发模式：每批消息数')
    parser.add_argument('--interval', type=int, default=60, help='突发模式：间隔秒数')
    args = parser.parse_args()

    if args.mode == 'burst':
        run_burst_mode(args.burst, args.interval)
    else:
        run_normal_mode(args.rate)
