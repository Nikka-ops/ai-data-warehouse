# -*- coding: utf-8 -*-
"""ETL：DWD → DWS → ADS（幂等 INSERT + OPTIMIZE FINAL）"""
import os, sys
import clickhouse_connect
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('etl_dws_ads')


@ch_retry
def get_client():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
    )


DWS_DAILY_SQL = """
INSERT INTO dws.order_daily
SELECT
    order_date                                              AS dt,
    countDistinct(order_id)                                 AS order_cnt,
    count()                                                 AS item_cnt,
    sum(price)                                              AS gmv,
    sum(freight_value)                                      AS freight_total,
    countDistinct(customer_unique_id)                       AS user_cnt,
    countDistinctIf(order_id, is_delivered = 1)             AS delivered_cnt,
    countDistinctIf(order_id, order_status = 'canceled')    AS cancel_cnt,
    round(sum(price) / countDistinct(order_id), 2)          AS avg_order_value,
    now()                                                   AS _load_time
FROM dwd.order_detail
GROUP BY order_date
ORDER BY order_date
"""

DWS_CATEGORY_SQL = """
INSERT INTO dws.category_daily
SELECT
    order_date                  AS dt,
    product_category,
    countDistinct(order_id)     AS order_cnt,
    sum(price)                  AS gmv,
    round(avg(price), 2)        AS avg_price,
    now()                       AS _load_time
FROM dwd.order_detail
GROUP BY order_date, product_category
ORDER BY order_date, product_category
"""

ADS_STATE_RANK_SQL = """
INSERT INTO ads.state_sales_rank
SELECT
    formatDateTime(order_date, '%Y-%m')     AS dt_month,
    state,
    sum(price)                              AS gmv,
    countDistinct(order_id)                 AS order_cnt,
    rank() OVER (
        PARTITION BY formatDateTime(order_date, '%Y-%m')
        ORDER BY sum(price) DESC
    )                                       AS rank_by_gmv,
    now()                                   AS _load_time
FROM dwd.order_detail
GROUP BY dt_month, state
"""


def _load_dws_daily(client):
    log.info('DWS 每日销售汇总...')
    client.command(DWS_DAILY_SQL)
    client.command('OPTIMIZE TABLE dws.order_daily FINAL')
    cnt = client.query('SELECT count() FROM dws.order_daily').first_row[0]
    log.info('dws.order_daily：%d 行（%d 天）', cnt, cnt)


def _load_dws_category(client):
    log.info('DWS 品类每日汇总...')
    client.command(DWS_CATEGORY_SQL)
    client.command('OPTIMIZE TABLE dws.category_daily FINAL')
    cnt = client.query('SELECT count() FROM dws.category_daily').first_row[0]
    cnt_cat = client.query('SELECT countDistinct(product_category) FROM dws.category_daily').first_row[0]
    log.info('dws.category_daily：%d 行，%d 个品类', cnt, cnt_cat)


def _load_ads_monthly_kpi(client) -> pd.DataFrame:
    log.info('ADS 月度 KPI...')
    df_daily = client.query_df("""
        SELECT formatDateTime(dt, '%Y-%m') AS ym,
               gmv, order_cnt, user_cnt, avg_order_value
        FROM dws.order_daily ORDER BY dt
    """)
    df = df_daily.groupby('ym', sort=True).agg(
        gmv=('gmv', 'sum'),
        order_cnt=('order_cnt', 'sum'),
        user_cnt=('user_cnt', 'sum'),
    ).reset_index()
    df['avg_order_value'] = round(df['gmv'] / df['order_cnt'], 2)
    df['prev_gmv'] = df['gmv'].shift(1)
    df['mom_gmv_rate'] = round(
        (df['gmv'] - df['prev_gmv']) / df['prev_gmv'].where(df['prev_gmv'] != 0) * 100, 2
    )
    df['_load_time'] = datetime.now()
    df = df.drop(columns=['prev_gmv'])

    # 幂等：用 ReplacingMergeTree 版本覆盖（_load_time 递增）
    client.insert_df('ads.monthly_kpi', df)
    client.command('OPTIMIZE TABLE ads.monthly_kpi FINAL')

    cnt = client.query('SELECT count() FROM ads.monthly_kpi').first_row[0]
    log.info('ads.monthly_kpi：%d 个月', cnt)
    return df


def _load_ads_state_rank(client):
    log.info('ADS 省份销售排行...')
    client.command(ADS_STATE_RANK_SQL)
    client.command('OPTIMIZE TABLE ads.state_sales_rank FINAL')
    cnt = client.query('SELECT count() FROM ads.state_sales_rank').first_row[0]
    log.info('ads.state_sales_rank：%d 行', cnt)


def run_dws_ads_load():
    log.info('========== DWS/ADS 层聚合开始 ==========')
    start = datetime.now()
    client = get_client()
    log.info('ClickHouse 连接成功')

    _load_dws_daily(client)
    _load_dws_category(client)
    df_kpi = _load_ads_monthly_kpi(client)
    _load_ads_state_rank(client)

    # 预览最近6个月
    log.info('月度 GMV 预览（最近 6 个月）：')
    for _, row in df_kpi.tail(6).iloc[::-1].iterrows():
        mom = f"{row['mom_gmv_rate']}%" if pd.notna(row['mom_gmv_rate']) else '-'
        log.info('  %s  GMV=%,.0f  订单=%d  环比=%s',
                 row['ym'], row['gmv'], int(row['order_cnt']), mom)

    elapsed = (datetime.now() - start).total_seconds()
    log.info('========== DWS/ADS 层完成（耗时 %.1f 秒）==========', elapsed)


if __name__ == '__main__':
    run_dws_ads_load()
