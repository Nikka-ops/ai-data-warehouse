#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lambda 架构 — 批处理 ETL
将 ods.orders_batch 按日/品类/州聚合写入 dws.batch_daily_stats。
支持增量模式（只聚合未做过的日期）。

运行：
  python batch/batch_etl.py            # 增量聚合
  python batch/batch_etl.py --full     # 全量重算（先清空再写）
"""
import os, sys, argparse
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('batch_etl')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=15, send_receive_timeout=300,
    )


def run_aggregation(full: bool = False):
    """聚合 ods.orders_batch → dws.batch_daily_stats"""
    ch = _get_ch()

    if full:
        log.info('全量模式：清空 dws.batch_daily_stats')
        ch.command('TRUNCATE TABLE IF EXISTS dws.batch_daily_stats')

    # 找出 ods.orders_batch 中有数据的日期
    date_rows = ch.query("""
        SELECT DISTINCT event_date, _batch_id
        FROM ods.orders_batch
        ORDER BY event_date
    """).result_rows

    if not date_rows:
        log.info('ods.orders_batch 无数据，跳过聚合')
        return 0

    # 已聚合的日期
    done_dates = set()
    if not full:
        done_rows = ch.query("""
            SELECT DISTINCT stat_date FROM dws.batch_daily_stats
        """).result_rows
        done_dates = {r[0] for r in done_rows}

    written = 0
    for event_date, batch_id in date_rows:
        if event_date in done_dates:
            continue

        # INSERT SELECT 批量聚合
        ch.command(f"""
            INSERT INTO dws.batch_daily_stats
            (stat_date, product_category, state, order_cnt, total_gmv,
             avg_price, cancel_cnt, unique_customers, unique_sellers, _batch_id, _created_at)
            SELECT
                toDate(event_time)                          AS stat_date,
                product_category,
                state,
                count(DISTINCT order_id)                    AS order_cnt,
                round(sum(price), 2)                        AS total_gmv,
                round(avg(price), 2)                        AS avg_price,
                countIf(order_status = 'canceled')          AS cancel_cnt,
                count(DISTINCT customer_id)                 AS unique_customers,
                count(DISTINCT seller_id)                   AS unique_sellers,
                '{batch_id}'                                AS _batch_id,
                now()                                       AS _created_at
            FROM ods.orders_batch
            WHERE event_date = '{event_date}'
            GROUP BY stat_date, product_category, state
        """)
        written += 1
        log.info('[batch_etl] %s 聚合完成', event_date)

    log.info('批量聚合完成：写入 %d 个日期', written)
    return written


def main():
    parser = argparse.ArgumentParser(description='Lambda 批处理 ETL')
    parser.add_argument('--full', action='store_true', help='全量重算')
    args = parser.parse_args()
    run_aggregation(full=args.full)


if __name__ == '__main__':
    main()
