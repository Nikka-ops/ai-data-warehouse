# -*- coding: utf-8 -*-
"""ETL：CSV 数据 → ClickHouse ODS 层（幂等，ReplacingMergeTree去重）"""
import os, sys
import pandas as pd
import clickhouse_connect
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('etl_ods')


@ch_retry
def get_client():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
    )


def _batch_insert(client, table: str, df: pd.DataFrame):
    total = len(df)
    for i in range(0, total, cfg.etl_batch_size):
        batch = df.iloc[i:i + cfg.etl_batch_size]
        client.insert_df(table, batch)
        pct = min(100, int((i + cfg.etl_batch_size) / total * 100))
        log.info('  写入进度：%d%% (%d/%d)', pct, min(i + cfg.etl_batch_size, total), total)


def _deduplicate(client, table: str):
    """触发 ReplacingMergeTree 去重合并，保证幂等性"""
    log.info('  触发 %s 去重合并...', table)
    client.command(f'OPTIMIZE TABLE {table} FINAL')


def load_orders(client) -> int:
    log.info('加载订单表...')
    path = os.path.join(cfg.data_dir, 'olist_orders_dataset.csv')
    df = pd.read_csv(path).rename(columns={
        'order_purchase_timestamp':       'order_purchase_ts',
        'order_approved_at':              'order_approved_ts',
        'order_delivered_customer_date':  'order_delivered_ts',
        'order_estimated_delivery_date':  'order_estimated_ts',
    })
    for col in ['order_purchase_ts', 'order_approved_ts', 'order_delivered_ts', 'order_estimated_ts']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    before = df.shape[0]
    df = df.dropna(subset=['order_purchase_ts'])
    dropped = before - df.shape[0]
    if dropped:
        log.warning('丢弃 %d 行（order_purchase_ts 为空）', dropped)

    df['_load_time'] = datetime.now()
    cols = ['order_id', 'customer_id', 'order_status',
            'order_purchase_ts', 'order_approved_ts',
            'order_delivered_ts', 'order_estimated_ts', '_load_time']
    _batch_insert(client, 'ods.orders_raw', df[cols])
    _deduplicate(client, 'ods.orders_raw')
    log.info('订单表完成，共 %d 行', len(df))
    return len(df)


def load_order_items(client) -> int:
    log.info('加载订单商品表...')
    path = os.path.join(cfg.data_dir, 'olist_order_items_dataset.csv')
    df = pd.read_csv(path).rename(columns={'shipping_limit_date': 'shipping_limit_ts'})
    df['shipping_limit_ts'] = pd.to_datetime(df['shipping_limit_ts'], errors='coerce')
    df['_load_time'] = datetime.now()
    cols = ['order_id', 'order_item_id', 'product_id', 'seller_id',
            'price', 'freight_value', 'shipping_limit_ts', '_load_time']
    _batch_insert(client, 'ods.order_items_raw', df[cols])
    _deduplicate(client, 'ods.order_items_raw')
    log.info('订单商品表完成，共 %d 行', len(df))
    return len(df)


def load_customers(client) -> int:
    log.info('加载客户表...')
    path = os.path.join(cfg.data_dir, 'olist_customers_dataset.csv')
    df = pd.read_csv(path).rename(columns={
        'customer_city':  'city',
        'customer_state': 'state',
    })
    df['_load_time'] = datetime.now()
    cols = ['customer_id', 'customer_unique_id', 'city', 'state', '_load_time']
    _batch_insert(client, 'ods.customers_raw', df[cols])
    _deduplicate(client, 'ods.customers_raw')
    log.info('客户表完成，共 %d 行', len(df))
    return len(df)


def load_products(client) -> int:
    log.info('加载商品表...')
    path = os.path.join(cfg.data_dir, 'olist_products_dataset.csv')
    df = pd.read_csv(path).rename(columns={'product_category_name': 'product_category_name'})
    df['product_category_name'] = df['product_category_name'].fillna('unknown')
    df['_load_time'] = datetime.now()
    cols = ['product_id', 'product_category_name', 'product_weight_g',
            'product_length_cm', 'product_height_cm', 'product_width_cm', '_load_time']
    _batch_insert(client, 'ods.products_raw', df[cols])
    _deduplicate(client, 'ods.products_raw')
    log.info('商品表完成，共 %d 行', len(df))
    return len(df)


def verify_counts(client):
    tables = [
        ('ods.orders_raw',      '订单表'),
        ('ods.order_items_raw', '订单商品表'),
        ('ods.customers_raw',   '客户表'),
        ('ods.products_raw',    '商品表'),
    ]
    for table, name in tables:
        cnt = client.query(f'SELECT count() FROM {table}').first_row[0]
        log.info('  %s → %d 行', name, cnt)


def run_ods_load():
    log.info('========== ODS 层数据加载开始 ==========')
    start = datetime.now()
    client = get_client()
    log.info('ClickHouse 连接成功 (%s:%d)', cfg.ch_host, cfg.ch_port)

    load_orders(client)
    load_order_items(client)
    load_customers(client)
    load_products(client)

    verify_counts(client)
    elapsed = (datetime.now() - start).total_seconds()
    log.info('========== ODS 层加载完成（耗时 %.1f 秒）==========', elapsed)


if __name__ == '__main__':
    run_ods_load()
