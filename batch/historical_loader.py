#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lambda 架构 — 离线历史数据加载器

两种模式：
  --mode python : Python 逐行生成（真实分布，适合 < 500万行）
  --mode sql    : ClickHouse 原生 SQL 批量生成（亿级/TB 级，速度极快）

TB 级数据估算（sql 模式）：
  1亿行 × 约 200 字节/行 ≈ 20GB 原始，ClickHouse 压缩后约 2-4GB
  10亿行 ≈ 数十GB，Parquet 等效约 100-200GB，可称"亿级数仓数据"

运行：
  # 标准演示（90天×3万/天 = 270万行）
  python batch/historical_loader.py --days 90 --daily-orders 30000

  # 亿级数据（直接在 ClickHouse 内生成，约10-30分钟）
  python batch/historical_loader.py --mode sql --rows 100000000

  # TB 级（ClickHouse 压缩后数十GB，原始等效约100GB）
  python batch/historical_loader.py --mode sql --rows 500000000
"""
import os, sys, uuid, random, argparse
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('historical_loader')

# ── 数据分布（与 kafka/producer.py 保持一致）──────────────────
STATES = {
    'SP': 0.42, 'RJ': 0.13, 'MG': 0.11, 'RS': 0.06, 'PR': 0.06,
    'SC': 0.04, 'BA': 0.04, 'GO': 0.03, 'ES': 0.02, 'PE': 0.02,
    'CE': 0.02, 'AM': 0.01, 'MT': 0.01, 'MS': 0.01, 'DF': 0.02,
}
STATE_LIST    = list(STATES.keys())
STATE_WEIGHTS = list(STATES.values())

CITIES = {
    'SP': ['São Paulo', 'Campinas', 'Guarulhos'],
    'RJ': ['Rio de Janeiro', 'Niterói', 'Nova Iguaçu'],
    'MG': ['Belo Horizonte', 'Uberlândia', 'Contagem'],
    'RS': ['Porto Alegre', 'Caxias do Sul', 'Pelotas'],
    'PR': ['Curitiba', 'Londrina', 'Maringá'],
    'SC': ['Florianópolis', 'Joinville', 'Blumenau'],
    'BA': ['Salvador', 'Feira de Santana', 'Vitória da Conquista'],
    'GO': ['Goiânia', 'Aparecida de Goiânia', 'Anápolis'],
    'ES': ['Vitória', 'Vila Velha', 'Serra'],
    'PE': ['Recife', 'Caruaru', 'Petrolina'],
    'CE': ['Fortaleza', 'Caucaia', 'Juazeiro do Norte'],
    'AM': ['Manaus', 'Parintins', 'Itacoatiara'],
    'MT': ['Cuiabá', 'Várzea Grande', 'Rondonópolis'],
    'MS': ['Campo Grande', 'Dourados', 'Três Lagoas'],
    'DF': ['Brasília', 'Taguatinga', 'Ceilândia'],
}

CATEGORIES = {
    'cama_mesa_banho':        (0.14, 80,  350,  15, 30),
    'beleza_saude':           (0.13, 30,  200,  10, 20),
    'esporte_lazer':          (0.12, 50,  400,  20, 40),
    'informatica_acessorios': (0.10, 80,  800,  30, 60),
    'moveis_decoracao':       (0.09, 150, 1500, 50, 120),
    'utilidades_domesticas':  (0.08, 40,  250,  15, 30),
    'relogios_presentes':     (0.07, 100, 600,  20, 40),
    'telefonia':              (0.06, 200, 2000, 30, 50),
    'automotivo':             (0.05, 80,  500,  25, 50),
    'brinquedos':             (0.04, 30,  200,  10, 25),
    'garden_tools':           (0.04, 60,  400,  20, 40),
    'pet_shop':               (0.03, 30,  150,  10, 20),
    'livros':                 (0.03, 20,  120,   8, 15),
}
CAT_NAMES   = list(CATEGORIES.keys())
CAT_WEIGHTS = [v[0] for v in CATEGORIES.values()]

STATUSES       = ['created','approved','processing','shipped','delivered','canceled','unavailable']
STATUS_WEIGHTS = [0.05, 0.08, 0.10, 0.15, 0.55, 0.06, 0.01]
PAYMENT_TYPES  = ['credit_card', 'boleto', 'debit_card', 'voucher']
PAY_WEIGHTS    = [0.73, 0.19, 0.05, 0.03]

HOUR_WEIGHTS    = [0.3,0.2,0.15,0.1,0.1,0.15, 0.3,0.5,0.7,0.9,1.0,1.0,
                   0.95,0.9,0.85,0.8,0.85,0.9, 1.0,1.1,1.2,1.1,0.9,0.6]
HOUR_WEIGHT_SUM = sum(HOUR_WEIGHTS)
DOW_WEIGHTS     = [0.85, 0.85, 0.90, 0.90, 1.0, 1.2, 1.1]

SELLERS       = [f'S{i:04d}' for i in range(1, 501)]
PRODUCTS_HOT  = [f'P{i:04d}' for i in range(1, 201)]
PRODUCTS_LONG = [f'P{i:04d}' for i in range(201, 1001)]
CHUNK_SIZE    = 10_000


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=15, send_receive_timeout=600,
    )


# ══════════════════════════════════════════════════════════════
# 模式1：ClickHouse SQL 亿级/TB级生成（推荐）
# 直接在 ClickHouse 内 INSERT SELECT from numbers()，无 Python 传输开销
# 1亿行约需 5-15分钟，5亿行约需 30-60分钟（取决于服务器配置）
# ══════════════════════════════════════════════════════════════

# 品类数组（13个，ClickHouse SQL 用 arrayElement + modulo 选取）
_CAT_ARRAY = (
    "['cama_mesa_banho','beleza_saude','esporte_lazer','informatica_acessorios',"
    "'moveis_decoracao','utilidades_domesticas','relogios_presentes','telefonia',"
    "'automotivo','brinquedos','garden_tools','pet_shop','livros']"
)
_STATE_ARRAY = (
    "['SP','SP','SP','SP','RJ','RJ','MG','MG','RS','PR','SC','BA','GO','ES','PE',"
    "'CE','AM','MT','MS','DF','SP','RJ','MG','SP','SP']"   # SP权重更高
)
_STATUS_ARRAY = (
    "['delivered','delivered','delivered','delivered','delivered','shipped',"
    "'processing','approved','canceled','created','unavailable']"
)
_PAY_ARRAY = (
    "['credit_card','credit_card','credit_card','credit_card','credit_card',"
    "'credit_card','credit_card','boleto','boleto','debit_card','voucher']"
)

# 时间范围：过去2年（730天）
_SQL_BATCH_ORDERS = """
INSERT INTO ods.orders_batch
    (order_id, customer_id, product_id, product_category, seller_id,
     price, freight_value, order_status, state, city,
     event_time, event_date, _batch_id, _load_time)
SELECT
    lower(hex(generateUUIDv4()))                                         AS order_id,
    lower(hex(generateUUIDv4()))                                         AS customer_id,
    concat('P', leftPad(toString(1 + cityHash64(number*7+1) % 1000), 4, '0')) AS product_id,
    arrayElement({cat_array}, 1 + cityHash64(number*3+2) % 13)          AS product_category,
    concat('S', leftPad(toString(1 + cityHash64(number*11+3) % 500), 4, '0')) AS seller_id,
    round(20.0 + (cityHash64(number*5+4) % 19800) / 10.0, 2)           AS price,
    round(8.0  + (cityHash64(number*7+5) % 920)  / 10.0, 2)            AS freight_value,
    arrayElement({status_array}, 1 + cityHash64(number*13+6) % 11)      AS order_status,
    arrayElement({state_array},  1 + cityHash64(number*17+7) % 25)      AS state,
    'Brazil'                                                             AS city,
    toDateTime('2023-01-01 00:00:00') +
        toIntervalSecond(cityHash64(number*19+8) % {span_seconds})       AS event_time,
    toDate(toDateTime('2023-01-01 00:00:00') +
        toIntervalSecond(cityHash64(number*19+8) % {span_seconds}))      AS event_date,
    'sql_bulk'                                                           AS _batch_id,
    now()                                                                AS _load_time
FROM numbers({total_rows})
SETTINGS
    max_insert_threads = 4,
    max_block_size = 1000000
""".format(
    cat_array    = _CAT_ARRAY,
    state_array  = _STATE_ARRAY,
    status_array = _STATUS_ARRAY,
    span_seconds = 2 * 365 * 24 * 3600,   # 覆盖2年时间段
    total_rows   = '{total_rows}',         # 占位符，运行时替换
)

_SQL_BATCH_PAYMENTS = """
INSERT INTO ods.payments_batch
    (payment_id, order_id, payment_type, payment_value,
     installments, event_date, _batch_id, _load_time)
SELECT
    lower(hex(generateUUIDv4()))                                         AS payment_id,
    lower(hex(generateUUIDv4()))                                         AS order_id,
    arrayElement({pay_array}, 1 + cityHash64(number*23+9) % 11)         AS payment_type,
    round(30.0 + (cityHash64(number*29+10) % 19700) / 10.0, 2)         AS payment_value,
    toUInt8(1 + cityHash64(number*31+11) % 12)                          AS installments,
    toDate(toDateTime('2023-01-01 00:00:00') +
        toIntervalSecond(cityHash64(number*19+8) % {span_seconds}))      AS event_date,
    'sql_bulk'                                                           AS _batch_id,
    now()                                                                AS _load_time
FROM numbers({total_rows})
SETTINGS
    max_insert_threads = 4,
    max_block_size = 1000000
""".format(
    pay_array    = _PAY_ARRAY,
    span_seconds = 2 * 365 * 24 * 3600,
    total_rows   = '{total_rows}',
)


def load_sql_bulk(total_rows: int = 100_000_000):
    """
    使用 ClickHouse 原生 numbers() 函数生成亿级历史数据。
    数据直接在 ClickHouse 服务端生成，无网络传输瓶颈。

    数据规模参考：
      1000万行  ≈ 1-3 分钟，约 300MB 磁盘
      1亿行    ≈ 10-20 分钟，约 3GB 磁盘
      5亿行    ≈ 60-120 分钟，约 15GB 磁盘
      10亿行   ≈ 120-240 分钟，约 30GB 磁盘（ClickHouse 压缩后）
    """
    ch = _get_ch()

    # 检查是否已有 sql_bulk 数据（幂等）
    existing = ch.query(
        "SELECT count() FROM ods.orders_batch WHERE _batch_id = 'sql_bulk'"
    ).first_row[0]
    if existing > 0:
        log.info('sql_bulk 数据已存在 %d 行，跳过', existing)
        return existing

    log.info('开始 ClickHouse SQL 批量生成 %s 行历史订单...', f'{total_rows:,}')

    # 分批执行，避免单次 INSERT 过大（每批 5000万）
    batch_size = 50_000_000
    written = 0
    for start in range(0, total_rows, batch_size):
        chunk = min(batch_size, total_rows - start)
        log.info('  生成订单 %s-%s...', f'{start:,}', f'{start+chunk:,}')

        # 每批用不同偏移量（通过调整 number 起点实现去重）
        offset_sql = _SQL_BATCH_ORDERS.replace(
            'FROM numbers({total_rows})',
            f'FROM numbers({start}, {chunk})',
        )
        ch.command(offset_sql)

        pay_sql = _SQL_BATCH_PAYMENTS.replace(
            'FROM numbers({total_rows})',
            f'FROM numbers({start+1}, {chunk})',
        )
        ch.command(pay_sql)

        written += chunk
        log.info('  已写入 %s / %s 行', f'{written:,}', f'{total_rows:,}')

    log.info('SQL 批量生成完成：%s 行', f'{total_rows:,}')
    return total_rows


# ══════════════════════════════════════════════════════════════
# 模式2：Python 生成（真实分布，适合 < 500万行）
# ══════════════════════════════════════════════════════════════

def _pick_hour(daily_orders: int) -> list[int]:
    hours = []
    for h, w in enumerate(HOUR_WEIGHTS):
        cnt = max(0, round(daily_orders * w / HOUR_WEIGHT_SUM))
        hours.extend([h] * cnt)
    random.shuffle(hours)
    return hours[:daily_orders]


def _gen_order(order_date: date, hour: int, batch_id: str) -> tuple[dict, dict]:
    cat_name = random.choices(CAT_NAMES, weights=CAT_WEIGHTS)[0]
    _, price_lo, price_hi, freight_lo, freight_hi = CATEGORIES[cat_name]
    price       = round(random.uniform(price_lo, price_hi), 2)
    freight     = round(random.uniform(freight_lo, freight_hi), 2)
    state       = random.choices(STATE_LIST, weights=STATE_WEIGHTS)[0]
    city        = random.choice(CITIES[state])
    status      = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
    product_id  = random.choices(
        [random.choice(PRODUCTS_HOT), random.choice(PRODUCTS_LONG)],
        weights=[0.8, 0.2]
    )[0]
    event_dt    = datetime(order_date.year, order_date.month, order_date.day,
                           hour, random.randint(0,59), random.randint(0,59))
    order_id    = str(uuid.uuid4())
    customer_id = str(uuid.uuid4())
    seller_id   = random.choice(SELLERS)
    order = {
        'order_id': order_id, 'customer_id': customer_id, 'product_id': product_id,
        'product_category': cat_name, 'seller_id': seller_id,
        'price': price, 'freight_value': freight, 'order_status': status,
        'state': state, 'city': city, 'event_time': event_dt,
        'event_date': order_date, '_batch_id': batch_id, '_load_time': datetime.now(),
    }
    pay_type    = random.choices(PAYMENT_TYPES, weights=PAY_WEIGHTS)[0]
    installments = random.choice([1,1,1,2,3,6,12]) if pay_type == 'credit_card' else 1
    payment = {
        'payment_id': str(uuid.uuid4()), 'order_id': order_id,
        'payment_type': pay_type, 'payment_value': round(price+freight, 2),
        'installments': installments, 'event_date': order_date,
        '_batch_id': batch_id, '_load_time': datetime.now(),
    }
    return order, payment


def load_python(days: int = 90, daily_orders: int = 30_000):
    ch = _get_ch()
    today = date.today()
    total = 0

    for delta in range(days, 0, -1):
        target_date = today - timedelta(days=delta)
        batch_id    = target_date.strftime('%Y%m%d')

        existing = ch.query(f"""
            SELECT count() FROM ods.orders_batch WHERE _batch_id = '{batch_id}'
        """).first_row[0]
        if existing > 0:
            log.info('[%s] 已存在 %d 行，跳过', batch_id, existing)
            continue

        dow_factor = DOW_WEIGHTS[target_date.weekday()]
        day_cnt    = max(1000, int(daily_orders * dow_factor))
        hours      = _pick_hour(day_cnt)
        order_rows, pay_rows = [], []

        for h in hours:
            order, pay = _gen_order(target_date, h, batch_id)
            order_rows.append([
                order['order_id'], order['customer_id'], order['product_id'],
                order['product_category'], order['seller_id'],
                order['price'], order['freight_value'], order['order_status'],
                order['state'], order['city'], order['event_time'],
                order['event_date'], order['_batch_id'], order['_load_time'],
            ])
            pay_rows.append([
                pay['payment_id'], pay['order_id'], pay['payment_type'],
                pay['payment_value'], pay['installments'],
                pay['event_date'], pay['_batch_id'], pay['_load_time'],
            ])

        for i in range(0, len(order_rows), CHUNK_SIZE):
            ch.insert('ods.orders_batch', order_rows[i:i+CHUNK_SIZE],
                column_names=['order_id','customer_id','product_id','product_category',
                              'seller_id','price','freight_value','order_status',
                              'state','city','event_time','event_date',
                              '_batch_id','_load_time'])
        for i in range(0, len(pay_rows), CHUNK_SIZE):
            ch.insert('ods.payments_batch', pay_rows[i:i+CHUNK_SIZE],
                column_names=['payment_id','order_id','payment_type','payment_value',
                              'installments','event_date','_batch_id','_load_time'])

        total += len(order_rows)
        log.info('[%s] 写入 %d 行（累计 %d）', batch_id, len(order_rows), total)

    log.info('Python 模式加载完成：%d 行', total)
    return total


def main():
    parser = argparse.ArgumentParser(description='Lambda 历史数据加载器')
    parser.add_argument('--mode', choices=['python', 'sql'], default='python',
                        help='python=真实分布(<500万) sql=ClickHouse原生(亿级/TB级)')
    parser.add_argument('--days',         type=int, default=90,
                        help='[python模式] 加载天数')
    parser.add_argument('--daily-orders', type=int, default=30_000,
                        help='[python模式] 每日订单量')
    parser.add_argument('--rows',         type=int, default=100_000_000,
                        help='[sql模式] 总行数（默认1亿）')
    parser.add_argument('--skip-agg',    action='store_true',
                        help='跳过批量聚合（仅加载原始数据）')
    args = parser.parse_args()

    if args.mode == 'sql':
        log.info('SQL 模式：生成 %s 行数据', f'{args.rows:,}')
        n = load_sql_bulk(total_rows=args.rows)
    else:
        log.info('Python 模式：%d 天 × %d 行/天', args.days, args.daily_orders)
        n = load_python(days=args.days, daily_orders=args.daily_orders)

    if not args.skip_agg:
        log.info('触发批量聚合...')
        from batch.batch_etl import run_aggregation
        run_aggregation()

    log.info('完成：共 %s 行', f'{n:,}')


if __name__ == '__main__':
    main()
