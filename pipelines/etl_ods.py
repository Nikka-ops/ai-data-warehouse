"""
ETL 脚本：将 Kaggle CSV 数据加载到 ClickHouse ODS 层
支持批量写入，有进度提示
"""

import os
import pandas as pd
import clickhouse_connect
from datetime import datetime

# ── 配置 ──────────────────────────────────────────────────────
CH_HOST     = os.getenv('CLICKHOUSE_HOST', 'localhost')
CH_PORT     = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_USER     = os.getenv('CLICKHOUSE_USER', 'admin')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')
DATA_DIR    = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
BATCH_SIZE  = 10_000


def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )


# ── 数据加载函数 ───────────────────────────────────────────────

def load_orders(client):
    """加载订单表到 ods.orders_raw"""
    print("\n📦 加载订单表...")
    path = os.path.join(DATA_DIR, 'olist_orders_dataset.csv')
    df = pd.read_csv(path)

    # 字段重命名
    df = df.rename(columns={
        'order_id':                          'order_id',
        'customer_id':                       'customer_id',
        'order_status':                      'order_status',
        'order_purchase_timestamp':          'order_purchase_ts',
        'order_approved_at':                 'order_approved_ts',
        'order_delivered_customer_date':     'order_delivered_ts',
        'order_estimated_delivery_date':     'order_estimated_ts',
    })

    # 时间字段处理
    for col in ['order_purchase_ts', 'order_approved_ts',
                'order_delivered_ts', 'order_estimated_ts']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 必须有 order_purchase_ts
    df = df.dropna(subset=['order_purchase_ts'])
    df['_load_time'] = datetime.now()

    cols = ['order_id', 'customer_id', 'order_status',
            'order_purchase_ts', 'order_approved_ts',
            'order_delivered_ts', 'order_estimated_ts', '_load_time']
    df = df[cols]

    _batch_insert(client, 'ods.orders_raw', df)
    print(f"  ✅ 订单表加载完成，共 {len(df):,} 行")
    return len(df)


def load_order_items(client):
    """加载订单商品表到 ods.order_items_raw"""
    print("\n🛒 加载订单商品表...")
    path = os.path.join(DATA_DIR, 'olist_order_items_dataset.csv')
    df = pd.read_csv(path)

    df = df.rename(columns={
        'order_id':             'order_id',
        'order_item_id':        'order_item_id',
        'product_id':           'product_id',
        'seller_id':            'seller_id',
        'price':                'price',
        'freight_value':        'freight_value',
        'shipping_limit_date':  'shipping_limit_ts',
    })

    df['shipping_limit_ts'] = pd.to_datetime(df['shipping_limit_ts'], errors='coerce')
    df['_load_time'] = datetime.now()

    cols = ['order_id', 'order_item_id', 'product_id', 'seller_id',
            'price', 'freight_value', 'shipping_limit_ts', '_load_time']
    df = df[cols]

    _batch_insert(client, 'ods.order_items_raw', df)
    print(f"  ✅ 订单商品表加载完成，共 {len(df):,} 行")
    return len(df)


def load_customers(client):
    """加载客户表到 ods.customers_raw"""
    print("\n👤 加载客户表...")
    path = os.path.join(DATA_DIR, 'olist_customers_dataset.csv')
    df = pd.read_csv(path)

    df = df.rename(columns={
        'customer_id':          'customer_id',
        'customer_unique_id':   'customer_unique_id',
        'customer_city':        'city',
        'customer_state':       'state',
    })

    df['_load_time'] = datetime.now()
    df = df[['customer_id', 'customer_unique_id', 'city', 'state', '_load_time']]

    _batch_insert(client, 'ods.customers_raw', df)
    print(f"  ✅ 客户表加载完成，共 {len(df):,} 行")
    return len(df)


def load_products(client):
    """加载商品表到 ods.products_raw"""
    print("\n📦 加载商品表...")
    path = os.path.join(DATA_DIR, 'olist_products_dataset.csv')
    df = pd.read_csv(path)

    df = df.rename(columns={
        'product_id':               'product_id',
        'product_category_name':    'product_category_name',
        'product_weight_g':         'product_weight_g',
        'product_length_cm':        'product_length_cm',
        'product_height_cm':        'product_height_cm',
        'product_width_cm':         'product_width_cm',
    })

    df['product_category_name'] = df['product_category_name'].fillna('unknown')
    df['_load_time'] = datetime.now()

    cols = ['product_id', 'product_category_name', 'product_weight_g',
            'product_length_cm', 'product_height_cm', 'product_width_cm', '_load_time']
    df = df[cols]

    _batch_insert(client, 'ods.products_raw', df)
    print(f"  ✅ 商品表加载完成，共 {len(df):,} 行")
    return len(df)


# ── 工具函数 ──────────────────────────────────────────────────

def _batch_insert(client, table: str, df: pd.DataFrame):
    """分批写入 ClickHouse，避免内存溢出"""
    total = len(df)
    for i in range(0, total, BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]
        client.insert_df(table, batch)
        pct = min(100, int((i + BATCH_SIZE) / total * 100))
        print(f"  ⏳ 写入进度：{pct}% ({min(i+BATCH_SIZE, total):,}/{total:,})", end='\r')
    print()


def verify_counts(client):
    """验证各表数据量"""
    print("\n📊 数据验证：")
    tables = [
        ('ods.orders_raw',      '订单表'),
        ('ods.order_items_raw', '订单商品表'),
        ('ods.customers_raw',   '客户表'),
        ('ods.products_raw',    '商品表'),
    ]
    for table, name in tables:
        cnt = client.query(f'SELECT count() FROM {table}').first_row[0]
        print(f"  {name:10s} → {cnt:>10,} 行")


# ── 主入口 ────────────────────────────────────────────────────

def run_ods_load():
    """执行 ODS 层全量加载"""
    print("=" * 55)
    print("  AI 数仓 - ODS 层数据加载")
    print(f"  时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    client = get_client()
    print(f"✅ ClickHouse 连接成功 ({CH_HOST}:{CH_PORT})")

    load_orders(client)
    load_order_items(client)
    load_customers(client)
    load_products(client)

    verify_counts(client)
    print("\n🎉 ODS 层加载完成！")


if __name__ == '__main__':
    run_ods_load()
