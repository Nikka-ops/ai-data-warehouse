# -*- coding: utf-8 -*-
"""
多模式实时订单生产者
支持：normal / peak / flash_sale / stress / anomaly / scenario（自动场景编排）
真实数据分布：Pareto 商品热度、时段权重、品类价格合理区间
"""

import json, random, time, uuid, argparse, math
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger

log = get_logger('producer')

# ── 地理分布（真实巴西电商权重）──────────────────────────────
STATES = {
    'SP': 0.42, 'RJ': 0.13, 'MG': 0.11, 'RS': 0.06,
    'PR': 0.05, 'SC': 0.04, 'BA': 0.04, 'GO': 0.03,
    'DF': 0.03, 'PE': 0.02, 'CE': 0.02, 'AM': 0.01,
    'ES': 0.01, 'MT': 0.01, 'MS': 0.01, 'PA': 0.01,
}

CITIES = {
    'SP': ['Sao Paulo', 'Campinas', 'Santos', 'Ribeirao Preto', 'Sorocaba'],
    'RJ': ['Rio de Janeiro', 'Niteroi', 'Petropolis', 'Nova Iguacu'],
    'MG': ['Belo Horizonte', 'Uberlandia', 'Contagem', 'Juiz de Fora'],
    'RS': ['Porto Alegre', 'Caxias do Sul', 'Pelotas', 'Canoas'],
    'PR': ['Curitiba', 'Londrina', 'Maringa'],
    'SC': ['Florianopolis', 'Joinville', 'Blumenau'],
    'BA': ['Salvador', 'Feira de Santana', 'Vitoria da Conquista'],
    'GO': ['Goiania', 'Aparecida de Goiania'],
    'DF': ['Brasilia'],
    'PE': ['Recife', 'Olinda', 'Caruaru'],
}

# ── 品类配置（权重 + 价格区间 + 运费区间）─────────────────────
CATEGORIES = {
    'beleza_saude':           {'weight': 0.15, 'price': (20,  300),  'freight': (8,  25)},
    'relogios_presentes':     {'weight': 0.10, 'price': (50,  800),  'freight': (10, 30)},
    'cama_mesa_banho':        {'weight': 0.11, 'price': (30,  500),  'freight': (12, 40)},
    'esporte_lazer':          {'weight': 0.10, 'price': (40,  400),  'freight': (10, 35)},
    'informatica_acessorios': {'weight': 0.09, 'price': (30,  600),  'freight': (8,  20)},
    'moveis_decoracao':       {'weight': 0.08, 'price': (80,  1500), 'freight': (30, 100)},
    'utilidades_domesticas':  {'weight': 0.07, 'price': (15,  200),  'freight': (8,  20)},
    'automotivo':             {'weight': 0.06, 'price': (30,  500),  'freight': (10, 30)},
    'brinquedos':             {'weight': 0.05, 'price': (20,  300),  'freight': (10, 25)},
    'telefonia':              {'weight': 0.07, 'price': (100, 3000), 'freight': (8,  15)},
    'eletronicos':            {'weight': 0.07, 'price': (80,  2000), 'freight': (8,  20)},
    'ferramentas_jardim':     {'weight': 0.05, 'price': (20,  400),  'freight': (12, 40)},
}

ORDER_STATUSES = ['created', 'approved', 'processing', 'shipped', 'delivered', 'canceled']
# 正常状态概率分布
NORMAL_STATUS_WEIGHTS = [0.05, 0.08, 0.10, 0.15, 0.57, 0.05]
PAYMENT_TYPES  = ['credit_card', 'boleto', 'voucher', 'debit_card']
PAYMENT_WEIGHTS = [0.75, 0.15, 0.06, 0.04]

# ── Pareto 商品热度池（20%商品贡献80%订单）──────────────────
_HOT_PRODUCTS  = [f"P{random.randint(100000, 400000)}" for _ in range(200)]   # 热门
_LONG_PRODUCTS = [f"P{random.randint(400001, 999999)}" for _ in range(800)]   # 长尾

def _pick_product() -> str:
    return random.choice(_HOT_PRODUCTS if random.random() < 0.8 else _LONG_PRODUCTS)

# ── 时段流量权重（模拟真实用户行为曲线）─────────────────────
HOUR_WEIGHTS = {
    0: 0.2,  1: 0.1,  2: 0.1,  3: 0.1,  4: 0.1,  5: 0.2,
    6: 0.4,  7: 0.7,  8: 1.0,  9: 1.4,  10: 1.6, 11: 1.5,
    12: 1.3, 13: 1.1, 14: 1.0, 15: 1.0, 16: 1.1, 17: 1.3,
    18: 1.5, 19: 1.8, 20: 2.0, 21: 1.7, 22: 1.2, 23: 0.7,
}


def _weighted_choice(d: dict) -> str:
    return random.choices(list(d.keys()), weights=list(d.values()), k=1)[0]


def _base_order(status_weights=None) -> dict:
    state    = _weighted_choice(STATES)
    city     = random.choice(CITIES.get(state, ['Unknown']))
    cat      = _weighted_choice({k: v['weight'] for k, v in CATEGORIES.items()})
    price    = round(random.uniform(*CATEGORIES[cat]['price']), 2)
    freight  = round(random.uniform(*CATEGORIES[cat]['freight']), 2)
    delay    = timedelta(minutes=random.randint(0, 5)) if random.random() < 0.05 else timedelta(0)
    return {
        'order_id':         str(uuid.uuid4()),
        'customer_id':      f"C{random.randint(10000, 99999)}",
        'product_id':       _pick_product(),
        'product_category': cat,
        'seller_id':        f"S{random.randint(1000, 9999)}",
        'price':            price,
        'freight_value':    freight,
        'order_status':     random.choices(ORDER_STATUSES,
                               weights=status_weights or NORMAL_STATUS_WEIGHTS)[0],
        'state':            state,
        'city':             city,
        'event_time':       (datetime.now() - delay).strftime('%Y-%m-%d %H:%M:%S'),
        'msg_version':      '2.0',
    }


def _base_payment(order_id: str, price: float, freight: float) -> dict:
    payment_type = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0]
    return {
        'payment_id':    str(uuid.uuid4()),
        'order_id':      order_id,
        'payment_type':  payment_type,
        'payment_value': round(price + freight, 2),
        'installments':  random.choices([1, 2, 3, 6, 12], weights=[0.40, 0.20, 0.20, 0.10, 0.10])[0],
        'event_time':    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'msg_version':   '2.0',
    }


# ══════════════════════════════════════════════════════════════
# 各模式消息生成器
# ══════════════════════════════════════════════════════════════

def gen_normal() -> tuple[dict, dict | None]:
    """正常业务订单，状态分布自然"""
    order = _base_order()
    pay   = _base_payment(order['order_id'], order['price'], order['freight_value']) \
            if random.random() < 0.82 else None
    return order, pay


def gen_peak() -> tuple[dict, dict | None]:
    """高峰期订单：更高成交率，热门品类集中"""
    order = _base_order(status_weights=[0.04, 0.07, 0.09, 0.15, 0.62, 0.03])
    # 高峰期热门品类权重翻倍
    if random.random() < 0.4:
        order['product_category'] = random.choice(
            ['beleza_saude', 'telefonia', 'eletronicos', 'relogios_presentes']
        )
    pay = _base_payment(order['order_id'], order['price'], order['freight_value']) \
          if random.random() < 0.90 else None
    return order, pay


def gen_flash_sale() -> tuple[dict, dict | None]:
    """限时秒杀：高并发 + 高取消率 + 单一品类集中 + 价格偏低"""
    flash_cat = random.choice(['beleza_saude', 'utilidades_domesticas', 'brinquedos'])
    price_lo, price_hi = CATEGORIES[flash_cat]['price']
    # 秒杀价格区间下浮50%
    price   = round(random.uniform(price_lo, price_lo + (price_hi - price_lo) * 0.5), 2)
    freight = round(random.uniform(*CATEGORIES[flash_cat]['freight']), 2)
    order = _base_order(status_weights=[0.10, 0.06, 0.08, 0.12, 0.44, 0.20])  # 取消率高
    order['product_category'] = flash_cat
    order['price']            = price
    order['freight_value']    = freight
    pay = _base_payment(order['order_id'], price, freight) if random.random() < 0.75 else None
    return order, pay


def gen_stress() -> tuple[dict, dict | None]:
    """压测模式：最简数据，最低延迟，最大吞吐"""
    order = {
        'order_id':         str(uuid.uuid4()),
        'customer_id':      f"C{random.randint(10000, 99999)}",
        'product_id':       random.choice(_HOT_PRODUCTS),
        'product_category': 'beleza_saude',
        'seller_id':        f"S{random.randint(1000, 9999)}",
        'price':            round(random.uniform(20, 200), 2),
        'freight_value':    10.0,
        'order_status':     'delivered',
        'state':            'SP',
        'city':             'Sao Paulo',
        'event_time':       datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'msg_version':      '2.0',
    }
    return order, None


def gen_anomaly() -> tuple[dict, dict | None]:
    """异常注入：负价格/空品类/非法状态，触发 AI ETL"""
    order = _base_order()
    anomaly_type = random.choice(['neg_price', 'null_category', 'bad_format', 'extreme_price'])
    if anomaly_type == 'neg_price':
        order['price'] = round(random.uniform(-100, -1), 2)
    elif anomaly_type == 'null_category':
        order['product_category'] = ''
    elif anomaly_type == 'bad_format':
        order['customer_id'] = f"INVALID_{random.randint(1, 99)}"
    elif anomaly_type == 'extreme_price':
        order['price'] = round(random.uniform(5000, 20000), 2)
    return order, None


# ══════════════════════════════════════════════════════════════
# Kafka Producer
# ══════════════════════════════════════════════════════════════

class OrderProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=cfg.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all', retries=3,
            batch_size=32768, linger_ms=5,
            compression_type='gzip',
        )
        self.counts = {'orders': 0, 'payments': 0, 'errors': 0}
        self.start  = time.time()

    def send(self, order: dict, payment: dict | None):
        try:
            self.producer.send(cfg.orders_topic, value=order)
            self.counts['orders'] += 1
            if payment:
                self.producer.send(cfg.payments_topic, value=payment)
                self.counts['payments'] += 1
        except Exception as e:
            self.counts['errors'] += 1
            log.error('发送失败：%s', e)

    def stats_line(self, mode: str) -> str:
        elapsed = max(time.time() - self.start, 0.01)
        return (
            f"[{datetime.now().strftime('%H:%M:%S')}] [{mode}] "
            f"订单 {self.counts['orders']:,}  "
            f"支付 {self.counts['payments']:,}  "
            f"速率 {self.counts['orders']/elapsed:.0f}/s  "
            f"失败 {self.counts['errors']}"
        )

    def close(self):
        self.producer.flush()
        self.producer.close()


# ══════════════════════════════════════════════════════════════
# 运行模式
# ══════════════════════════════════════════════════════════════

_MODE_GEN = {
    'normal':     (gen_normal,     10),
    'peak':       (gen_peak,       100),
    'flash_sale': (gen_flash_sale, 500),
    'stress':     (gen_stress,     1000),
    'anomaly':    (gen_anomaly,    20),
}

# 自动场景编排脚本（循环执行，模拟一个完整业务日）
SCENARIO_SCRIPT = [
    ('normal',     120),   # 2分钟 正常流量
    ('peak',       60),    # 1分钟 早高峰
    ('normal',     180),   # 3分钟 正常
    ('flash_sale', 30),    # 30秒  限时秒杀
    ('normal',     120),   # 2分钟 秒杀后回落
    ('anomaly',    15),    # 15秒  注入异常（触发 AI ETL）
    ('normal',     60),    # 1分钟 正常
    ('peak',       90),    # 1.5分钟 晚高峰
    ('normal',     60),    # 1分钟 正常
]


def _run_mode(producer: OrderProducer, mode: str, rate: int, duration_s: float):
    """以指定速率运行某模式 duration_s 秒"""
    gen_fn, _ = _MODE_GEN[mode]
    interval   = 1.0 / max(rate, 1)
    deadline   = time.time() + duration_s
    while time.time() < deadline:
        t0 = time.time()
        order, pay = gen_fn()
        producer.send(order, pay)
        elapsed = time.time() - t0
        sleep_t = max(0.0, interval - elapsed)
        if sleep_t > 0:
            time.sleep(sleep_t)


def run_single_mode(mode: str, rate: int):
    """固定模式持续运行"""
    log.info('[%s] 速率 %d/s，Ctrl+C 停止', mode, rate)
    p = OrderProducer()
    try:
        while True:
            gen_fn, _ = _MODE_GEN[mode]
            order, pay = gen_fn()
            p.send(order, pay)
            # 时段权重调整（仅 normal/peak 模式）
            if mode in ('normal', 'peak'):
                hw = HOUR_WEIGHTS.get(datetime.now().hour, 1.0)
                actual_rate = max(1, int(rate * hw))
            else:
                actual_rate = rate
            time.sleep(1.0 / actual_rate)
            if p.counts['orders'] % 500 == 0:
                log.info(p.stats_line(mode))
    except KeyboardInterrupt:
        pass
    finally:
        p.close()
        log.info('最终统计：%s', p.stats_line(mode))


def run_scenario():
    """自动场景编排：按脚本循环切换模式，模拟完整业务日"""
    log.info('场景编排模式启动，脚本共 %d 个阶段，循环执行', len(SCENARIO_SCRIPT))
    p = OrderProducer()
    round_n = 0
    try:
        while True:
            round_n += 1
            log.info('=== 第 %d 轮场景开始 ===', round_n)
            for mode, duration in SCENARIO_SCRIPT:
                _, default_rate = _MODE_GEN[mode]
                log.info('  → [%s] %d秒 @%d/s', mode, duration, default_rate)
                _run_mode(p, mode, default_rate, duration)
                log.info(p.stats_line(mode))
    except KeyboardInterrupt:
        pass
    finally:
        p.close()
        log.info('场景编排结束：%s', p.stats_line('scenario'))


# ══════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='多模式 Kafka 订单生产者')
    parser.add_argument('--mode', default='normal',
                        choices=list(_MODE_GEN.keys()) + ['scenario'],
                        help='运行模式（默认 normal）')
    parser.add_argument('--rate', type=int, default=0,
                        help='每秒消息数（0=使用模式默认值）')
    args = parser.parse_args()

    if args.mode == 'scenario':
        run_scenario()
    else:
        rate = args.rate or _MODE_GEN[args.mode][1]
        run_single_mode(args.mode, rate)
