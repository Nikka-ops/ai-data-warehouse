# -*- coding: utf-8 -*-
"""
业务库 ↔ 数仓 对账任务

实时链路最难自证的一件事是「数到底对不对」。
Exactly-Once、主键幂等、sequence 防乱序，这些都是设计上的保证，
但保证成不成立要有人去验。这个脚本就是那个验的人：
定时拿 MySQL 业务库和 Doris 数仓的同口径数字比一遍，差异落 ads.reconcile_result。

有了它，「数据一致性保证」才是一句可验证的话，
而不只是架构图上的一个箭头。

四个对账项，分别验链路的不同环节：

    order_cnt      订单数    验 CDC 有没有漏事件（快照表 vs 业务库）
    order_amount   订单金额  验金额字段有没有在类型转换中失真
    gmv            成交额    验 DWD→Kafka→DWS 窗口聚合这一段（窗口 vs 明细）
    pay_cnt        支付笔数  验支付流的过滤条件（SUCCESS）有没有算错

判定阈值分两级：
    差异 = 0        PASS
    |差异| < 1%     WARN —— 通常是"业务库还在写、数仓窗口还没关闭"的时间差，
                    对当天正在进行的时段属于正常
    否则            FAIL —— 需要人看

运行：
    python pipelines/reconcile.py                 # 对今天
    python pipelines/reconcile.py --date 2026-09-08
"""

import os
import sys
import argparse
from datetime import date, datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pymysql

from common.doris_client import get_client


MYSQL = dict(
    host=os.getenv('MYSQL_HOST', 'localhost'),
    port=int(os.getenv('MYSQL_PORT', '3306')),
    user=os.getenv('MYSQL_USER', 'root'),
    password=os.getenv('MYSQL_PASSWORD', 'root123'),
    database=os.getenv('MYSQL_DATABASE', 'mall'),
    charset='utf8mb4',
    autocommit=True,
)

WARN_PCT = 1.0      # 差异占比小于此值判 WARN，否则 FAIL


# ── 对账项定义 ────────────────────────────────────────────────
# 每项一对 SQL：业务库口径 / 数仓口径。写在一起是为了让"两边口径必须一致"
# 这件事在代码里一眼可见 —— 对账最容易出的错不是链路丢数，
# 而是两边 SQL 写的根本不是同一个东西。

CHECKS = [
    {
        'item': 'order_cnt',
        'desc': '订单数（业务库 order_info vs 数仓订单快照）',
        'mysql': """
            SELECT COUNT(*) FROM order_info WHERE DATE(create_time) = %s
        """,
        'doris': """
            SELECT COUNT(*) FROM dwd.order_snapshot WHERE create_date = %s
        """,
    },
    {
        'item': 'order_amount',
        'desc': '订单总额（验 DECIMAL 有没有在 CDC/JSON 传输中失真）',
        'mysql': """
            SELECT COALESCE(SUM(total_amount), 0) FROM order_info
            WHERE DATE(create_time) = %s
        """,
        'doris': """
            SELECT COALESCE(SUM(total_amount), 0) FROM dwd.order_snapshot
            WHERE create_date = %s
        """,
    },
    {
        'item': 'gmv',
        'desc': 'GMV（业务库订单明细 vs DWS 日累计窗口）',
        'mysql': """
            SELECT COALESCE(SUM(split_total_amount), 0) FROM order_detail
            WHERE DATE(create_time) = %s
        """,
        # 取当天 CUMULATE_1D 的最后一行 —— 累计窗口每分钟出一行，
        # 最新那行就是"截至此刻的当日累计"，不需要再 SUM
        'doris': """
            SELECT COALESCE(order_amount, 0) FROM dws.trade_window_agg
            WHERE stat_date = %s AND window_type = 'CUMULATE_1D'
              AND category1_name = 'ALL' AND region_name = 'ALL'
            ORDER BY window_start DESC LIMIT 1
        """,
    },
    {
        'item': 'pay_cnt',
        'desc': '支付笔数（业务库 payment_info SUCCESS vs DWS 支付累计窗口）',
        'mysql': """
            SELECT COUNT(DISTINCT order_id) FROM payment_info
            WHERE payment_status = 'SUCCESS' AND DATE(callback_time) = %s
        """,
        'doris': """
            SELECT COALESCE(pay_cnt, 0) FROM dws.pay_window_agg
            WHERE stat_date = %s AND window_type = 'CUMULATE_1D'
              AND payment_type = 'ALL'
            ORDER BY window_start DESC LIMIT 1
        """,
    },
]


def _scalar(rows, default=0.0) -> float:
    if not rows or rows[0][0] is None:
        return float(default)
    return float(rows[0][0])


def run_checks(day: date) -> list[dict]:
    d = day.isoformat()
    doris = get_client()
    if not doris.ping():
        raise RuntimeError('Doris 不可达')

    conn = pymysql.connect(**MYSQL)
    results = []
    try:
        with conn.cursor() as cur:
            for c in CHECKS:
                cur.execute(c['mysql'], (d,))
                src = _scalar(cur.fetchall())

                tgt = _scalar(doris.query(c['doris'], (d,)).result_rows)

                diff = tgt - src
                pct = (abs(diff) / src * 100) if src else (0.0 if not diff else 100.0)

                if diff == 0:
                    status = 'PASS'
                elif pct < WARN_PCT:
                    status = 'WARN'
                else:
                    status = 'FAIL'

                results.append({
                    'check_date':   d,
                    'check_item':   c['item'],
                    'check_time':   datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'source_value': round(src, 2),
                    'target_value': round(tgt, 2),
                    'diff_value':   round(diff, 2),
                    'diff_pct':     round(pct, 4),
                    'status':       status,
                    'detail':       c['desc'],
                })
    finally:
        conn.close()

    # 结果表是 Unique Key(check_date, check_item)，按主键覆盖，
    # 同一天重复对账只会刷新那一行，不会堆积历史噪声
    doris.stream_load('ads', 'reconcile_result', results)
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description='业务库与数仓对账')
    ap.add_argument('--date', help='对账日期 YYYY-MM-DD，默认今天')
    ap.add_argument('--strict', action='store_true',
                    help='有 FAIL 时以非零码退出（供调度判失败用）')
    args = ap.parse_args()

    day = (datetime.strptime(args.date, '%Y-%m-%d').date()
           if args.date else date.today())

    results = run_checks(day)

    icon = {'PASS': 'OK  ', 'WARN': 'WARN', 'FAIL': 'FAIL'}
    print(f'[对账] {day}')
    for r in results:
        print(f"  {icon[r['status']]} {r['check_item']:<14}"
              f" 业务库={r['source_value']:>14,.2f}"
              f" 数仓={r['target_value']:>14,.2f}"
              f" 差异={r['diff_value']:>12,.2f} ({r['diff_pct']:.4f}%)")

    failed = [r for r in results if r['status'] == 'FAIL']
    if failed:
        print(f'[对账] {len(failed)} 项不一致，详见 ads.reconcile_result')
        if args.strict:
            return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
