# -*- coding: utf-8 -*-
"""
业务库模拟器 —— 实时链路的源头

和常见的「Python 直接往 Kafka 灌 JSON」有本质区别：
这个程序只操作 MySQL 业务库，完全不知道 Kafka 和 Flink 的存在。
数据进入实时链路是 Flink CDC 读 binlog 的结果，
和真实生产环境里业务系统与数仓的关系一致 —— 业务系统不为数仓而写。

核心是一个订单状态机，而不是「随机生成一条带终态的订单」：

    CREATED ──85%──> PAID ──97%──> SHIPPED ──> DELIVERED
       │                 │
       └──15%──>CANCELED └──3%──> REFUNDED

每一次状态流转都是业务库上的一条 UPDATE，于是 CDC 捕获到的是
同一主键的多条变更。这才让下游的两件事有真实意义：
  * Doris Unique Key 按主键幂等  —— 同一订单会被写很多次
  * Sequence Column 防乱序覆盖   —— 迟到的旧状态不能把新状态改回去

时间被压缩了（真实世界的「下单到收货三天」在这里是几十秒），
否则跑一整天也看不到一个完整生命周期。

运行：
    python mock/business_simulator.py --rate 20        # 每秒新建 20 单
    python mock/business_simulator.py --rate 5 --burst # 周期性制造流量尖峰
    python mock/business_simulator.py --seed-dim       # 只初始化维表后退出
"""

import os
import random
import argparse
import time
from datetime import datetime, timedelta

import pymysql

MYSQL = dict(
    host=os.getenv('MYSQL_HOST', 'localhost'),
    port=int(os.getenv('MYSQL_PORT', '3306')),
    user=os.getenv('MYSQL_USER', 'root'),
    password=os.getenv('MYSQL_PASSWORD', 'root123'),
    database=os.getenv('MYSQL_DATABASE', 'mall'),
    charset='utf8mb4',
    autocommit=True,
)

# ── 业务参数 ──────────────────────────────────────────────────
PAY_RATE      = 0.85    # 下单后支付率
SHIP_RATE     = 0.97    # 支付后发货率（少量因缺货取消）
REFUND_RATE   = 0.03    # 支付后退款率
PAY_DELAY     = (3, 25)     # 秒；真实世界是分钟级
SHIP_DELAY    = (10, 60)
RECEIVE_DELAY = (20, 90)
REFUND_DELAY  = (30, 120)

PAYMENT_TYPES = ['ALIPAY', 'WECHAT', 'CARD', 'CREDIT']
PAYMENT_W     = [0.42, 0.40, 0.12, 0.06]
REFUND_REASON = ['商品质量问题', '不喜欢/不想要', '尺寸不合适', '发错货', '物流太慢']

SKU_POOL, USER_MAX, PROVINCE_IDS = [], 50000, list(range(1, 27))


def conn():
    return pymysql.connect(**MYSQL)


# ── 维表初始化 ────────────────────────────────────────────────

def seed_dimensions(c) -> None:
    """灌入商品与用户维表。品类和省份已由建表脚本写入。"""
    cur = c.cursor()
    cur.execute('SELECT COUNT(*) FROM sku_info')
    if cur.fetchone()[0] > 0:
        print('维表已存在，跳过初始化')
        return

    cur.execute('SELECT id, name FROM base_category3')
    cats = cur.fetchall()

    now = datetime.now()
    skus = []
    for i in range(1, 1001):
        cid, cname = random.choice(cats)
        # 不同品类价格量级差异明显，便于下游做异常价格检测
        base = {1: 3500, 2: 5500, 3: 2200, 4: 260, 5: 90, 6: 180, 7: 350,
                8: 210, 9: 60, 10: 45, 11: 55, 12: 3200, 13: 320,
                14: 45, 15: 180}.get(cid, 200)
        price = round(random.uniform(base * 0.5, base * 1.8), 2)
        skus.append((i, f'{cname}-商品{i:04d}', cid, random.randint(1, 40),
                     price, 1, now, now))

    cur.executemany(
        'INSERT INTO sku_info (id,sku_name,category3_id,tm_id,price,is_sale,'
        'create_time,update_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', skus)

    users = []
    for i in range(1, 5001):
        users.append((
            i, f'user_{i:06d}',
            random.choice(['M', 'F']),
            (now - timedelta(days=random.randint(6570, 20000))).date(),
            random.choices(['1', '2', '3'], weights=[0.7, 0.25, 0.05])[0],
            now, now,
        ))
    cur.executemany(
        'INSERT INTO user_info (id,login_name,gender,birthday,user_level,'
        'create_time,update_time) VALUES (%s,%s,%s,%s,%s,%s,%s)', users)

    print(f'维表初始化完成：{len(skus)} SKU / {len(users)} 用户')


def load_skus(c) -> None:
    global SKU_POOL
    cur = c.cursor()
    cur.execute('SELECT id, price FROM sku_info WHERE is_sale = 1')
    SKU_POOL = cur.fetchall()
    if not SKU_POOL:
        raise RuntimeError('sku_info 为空，请先执行 --seed-dim')


# ── 订单生命周期 ──────────────────────────────────────────────

class Pipeline:
    """维护在途订单，按到期时间推进状态"""

    def __init__(self, c):
        self.c = c
        self.pending = []     # [(due_ts, order_id, next_status)]
        self.stats = {'created': 0, 'paid': 0, 'shipped': 0,
                      'delivered': 0, 'canceled': 0, 'refunded': 0}

    # 下单：INSERT order_info + order_detail
    def create_order(self) -> None:
        cur = self.c.cursor()
        now = datetime.now()
        user_id = random.randint(1, 5000)
        province = random.choices(
            PROVINCE_IDS,
            weights=[3, 2, 2, 1, 4, 4, 4, 2, 2, 3, 5, 1, 1,
                     2, 2, 2, 3, 2, 1, 1, 2, 1, 1, 2, 1, 1])[0]

        items = random.choices([1, 2, 3, 4], weights=[0.6, 0.25, 0.1, 0.05])[0]
        picked = random.sample(SKU_POOL, min(items, len(SKU_POOL)))

        detail, total = [], 0.0
        for sku_id, price in picked:
            num = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
            amt = round(float(price) * num, 2)
            total += amt
            detail.append((sku_id, num, float(price), amt))

        freight = 0.0 if total > 99 else round(random.uniform(6, 15), 2)
        activity = round(total * random.choice([0, 0, 0, 0.05, 0.1]), 2)
        coupon = round(random.choice([0, 0, 0, 0, 10, 20, 50]), 2)
        payable = round(max(total - activity - coupon, 0.01) + freight, 2)

        cur.execute(
            'INSERT INTO order_info (user_id,province_id,order_status,total_amount,'
            'activity_reduce,coupon_reduce,freight_amount,create_time,update_time) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            (user_id, province, 'CREATED', payable, activity, coupon, freight, now, now))
        oid = cur.lastrowid

        cur.executemany(
            'INSERT INTO order_detail (order_id,sku_id,sku_num,order_price,'
            'split_total_amount,create_time,update_time) VALUES (%s,%s,%s,%s,%s,%s,%s)',
            [(oid, s, n, p, a, now, now) for s, n, p, a in detail])

        self.stats['created'] += 1

        # 决定这一单的下一步走向
        if random.random() < PAY_RATE:
            self._schedule(oid, 'PAID', PAY_DELAY)
        else:
            self._schedule(oid, 'CANCELED', (5, 40))

    def _schedule(self, oid: int, status: str, delay_range: tuple) -> None:
        due = time.time() + random.uniform(*delay_range)
        self.pending.append((due, oid, status))

    # 推进到期的状态流转 —— 每一次都是一条 UPDATE
    def advance(self) -> None:
        if not self.pending:
            return
        now_ts = time.time()
        due = [x for x in self.pending if x[0] <= now_ts]
        if not due:
            return
        self.pending = [x for x in self.pending if x[0] > now_ts]

        cur = self.c.cursor()
        for _, oid, status in due:
            now = datetime.now()

            if status == 'PAID':
                cur.execute(
                    'UPDATE order_info SET order_status=%s, payment_time=%s, '
                    'update_time=%s WHERE id=%s', ('PAID', now, now, oid))
                cur.execute('SELECT total_amount, user_id FROM order_info WHERE id=%s', (oid,))
                row = cur.fetchone()
                if row:
                    amt, uid = row
                    ptype = random.choices(PAYMENT_TYPES, weights=PAYMENT_W)[0]
                    cur.execute(
                        'INSERT INTO payment_info (order_id,user_id,payment_type,'
                        'payment_status,payment_amount,callback_time,create_time,update_time) '
                        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s) '
                        'ON DUPLICATE KEY UPDATE payment_status=VALUES(payment_status), '
                        'update_time=VALUES(update_time)',
                        (oid, uid, ptype, 'SUCCESS', amt, now, now, now))
                self.stats['paid'] += 1

                if random.random() < REFUND_RATE:
                    self._schedule(oid, 'REFUNDED', REFUND_DELAY)
                elif random.random() < SHIP_RATE:
                    self._schedule(oid, 'SHIPPED', SHIP_DELAY)
                else:
                    self._schedule(oid, 'CANCELED', (10, 30))

            elif status == 'SHIPPED':
                cur.execute(
                    'UPDATE order_info SET order_status=%s, ship_time=%s, '
                    'update_time=%s WHERE id=%s', ('SHIPPED', now, now, oid))
                self.stats['shipped'] += 1
                self._schedule(oid, 'DELIVERED', RECEIVE_DELAY)

            elif status == 'DELIVERED':
                cur.execute(
                    'UPDATE order_info SET order_status=%s, receive_time=%s, '
                    'update_time=%s WHERE id=%s', ('DELIVERED', now, now, oid))
                self.stats['delivered'] += 1

            elif status == 'CANCELED':
                cur.execute(
                    'UPDATE order_info SET order_status=%s, cancel_time=%s, '
                    'update_time=%s WHERE id=%s', ('CANCELED', now, now, oid))
                self.stats['canceled'] += 1

            elif status == 'REFUNDED':
                cur.execute(
                    'UPDATE order_info SET order_status=%s, update_time=%s '
                    'WHERE id=%s', ('REFUNDED', now, oid))
                cur.execute('SELECT total_amount, user_id FROM order_info WHERE id=%s', (oid,))
                row = cur.fetchone()
                if row:
                    amt, uid = row
                    cur.execute(
                        'INSERT INTO refund_info (order_id,user_id,refund_type,'
                        'refund_status,refund_amount,refund_reason,create_time,update_time) '
                        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                        (oid, uid,
                         random.choice(['ONLY_REFUND', 'RETURN_REFUND']),
                         'SUCCESS', amt, random.choice(REFUND_REASON), now, now))
                self.stats['refunded'] += 1

    def print_stats(self, elapsed: float) -> None:
        s = self.stats
        print(
            f"\r[{datetime.now():%H:%M:%S}] "
            f"下单 {s['created']:>6}  支付 {s['paid']:>6}  发货 {s['shipped']:>6}  "
            f"收货 {s['delivered']:>6}  取消 {s['canceled']:>5}  退款 {s['refunded']:>4}  "
            f"| 在途 {len(self.pending):>5}  {s['created']/max(elapsed,1):.1f} 单/秒",
            end='', flush=True)


def run(rate: int, burst: bool) -> None:
    c = conn()
    load_skus(c)
    pipe = Pipeline(c)

    print(f'业务库模拟器启动：{rate} 单/秒' + ('（含流量尖峰）' if burst else ''))
    print('数据写入 MySQL，由 Flink CDC 读 binlog 进入实时链路')
    print('Ctrl+C 停止\n')

    start = time.time()
    tick = 0
    try:
        while True:
            tick += 1
            n = rate
            # 周期性尖峰：模拟大促开场，用来观察窗口聚合与背压
            if burst and tick % 60 == 0:
                n = rate * 12
                print(f'\n[{datetime.now():%H:%M:%S}] 流量尖峰：本秒 {n} 单')

            for _ in range(n):
                pipe.create_order()
            pipe.advance()
            pipe.print_stats(time.time() - start)
            time.sleep(1)

    except KeyboardInterrupt:
        print('\n\n停止。最终统计：')
        for k, v in pipe.stats.items():
            print(f'  {k:<10} {v}')
    finally:
        c.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='电商业务库模拟器')
    ap.add_argument('--rate', type=int, default=10, help='每秒新建订单数')
    ap.add_argument('--burst', action='store_true', help='周期性流量尖峰')
    ap.add_argument('--seed-dim', action='store_true', help='仅初始化维表')
    args = ap.parse_args()

    if args.seed_dim:
        c = conn()
        seed_dimensions(c)
        c.close()
    else:
        c = conn()
        seed_dimensions(c)
        c.close()
        run(args.rate, args.burst)
