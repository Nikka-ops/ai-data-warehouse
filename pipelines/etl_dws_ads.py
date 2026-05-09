"""
ETL 脚本：DWD → DWS → ADS
聚合生成汇总层和应用层数据
"""

import os
import clickhouse_connect
import pandas as pd
from datetime import datetime

CH_HOST     = os.getenv('CLICKHOUSE_HOST', 'localhost')
CH_PORT     = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_USER     = os.getenv('CLICKHOUSE_USER', 'admin')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')


def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )


# ── DWS 层 SQL ───────────────────────────────────────────────

DWS_DAILY_SQL = """
INSERT INTO dws.order_daily
SELECT
    order_date                              AS dt,
    countDistinct(order_id)                 AS order_cnt,
    count()                                 AS item_cnt,
    sum(price)                              AS gmv,
    sum(freight_value)                      AS freight_total,
    countDistinct(customer_unique_id)       AS user_cnt,
    countDistinctIf(order_id, is_delivered = 1) AS delivered_cnt,
    countDistinctIf(order_id, order_status = 'canceled') AS cancel_cnt,
    round(sum(price) / countDistinct(order_id), 2) AS avg_order_value,
    now()                                   AS _load_time
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


def load_ads_monthly_kpi(client):
    """
    直接从 dws.order_daily 按月汇总（不嵌套聚合），
    环比在 Python 端用 pandas 计算。
    """
    print("🔄 ADS 月度 KPI...")

    # 直接读每日数据，不在 SQL 里做二次聚合
    df_daily = client.query_df("""
        SELECT
            formatDateTime(dt, '%Y-%m') AS ym,
            gmv,
            order_cnt,
            user_cnt,
            avg_order_value
        FROM dws.order_daily
        ORDER BY dt
    """)

    # Python 端按月汇总
    df = df_daily.groupby('ym', sort=True).agg(
        gmv=('gmv', 'sum'),
        order_cnt=('order_cnt', 'sum'),
        user_cnt=('user_cnt', 'sum'),
    ).reset_index()

    df['avg_order_value'] = round(df['gmv'] / df['order_cnt'], 2)

    # 计算环比
    df['prev_gmv'] = df['gmv'].shift(1)
    df['mom_gmv_rate'] = round(
        (df['gmv'] - df['prev_gmv']) / df['prev_gmv'].where(df['prev_gmv'] != 0) * 100, 2
    )
    df['_load_time'] = datetime.now()
    df = df.drop(columns=['prev_gmv'])

    # 写入 ClickHouse
    client.command("TRUNCATE TABLE ads.monthly_kpi")
    client.insert_df('ads.monthly_kpi', df)

    cnt = client.query("SELECT count() FROM ads.monthly_kpi").first_row[0]
    print(f"  ✅ ads.monthly_kpi：{cnt} 个月")
    return df


def run_dws_ads_load():
    print("=" * 55)
    print("  AI 数仓 - DWS / ADS 层聚合")
    print(f"  时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    client = get_client()
    print("✅ ClickHouse 连接成功")

    # DWS 层
    print("\n🔄 DWS 每日销售汇总...")
    client.command("TRUNCATE TABLE dws.order_daily")
    client.command(DWS_DAILY_SQL)
    cnt = client.query("SELECT count() FROM dws.order_daily").first_row[0]
    print(f"  ✅ dws.order_daily：{cnt:,} 行（{cnt} 天）")

    print("🔄 DWS 品类每日汇总...")
    client.command("TRUNCATE TABLE dws.category_daily")
    client.command(DWS_CATEGORY_SQL)
    cnt = client.query("SELECT count() FROM dws.category_daily").first_row[0]
    cnt_cat = client.query("SELECT countDistinct(product_category) FROM dws.category_daily").first_row[0]
    print(f"  ✅ dws.category_daily：{cnt:,} 行，{cnt_cat} 个品类")

    # ADS 层
    df_kpi = load_ads_monthly_kpi(client)

    print("🔄 ADS 省份销售排行...")
    client.command("TRUNCATE TABLE ads.state_sales_rank")
    client.command(ADS_STATE_RANK_SQL)
    cnt = client.query("SELECT count() FROM ads.state_sales_rank").first_row[0]
    print(f"  ✅ ads.state_sales_rank：{cnt:,} 行")

    # 预览
    print("\n📊 月度 GMV 预览（最近 6 个月）：")
    preview = df_kpi.tail(6).iloc[::-1]
    print(f"  {'月份':8s} {'GMV(元)':>14s} {'订单数':>8s} {'环比增长':>8s}")
    print("  " + "-" * 44)
    for _, row in preview.iterrows():
        mom = f"{row['mom_gmv_rate']}%" if pd.notna(row['mom_gmv_rate']) else "  -"
        print(f"  {row['ym']:8s} {row['gmv']:>14,.0f} {int(row['order_cnt']):>8,} {mom:>8s}")

    print("\n🎉 DWS / ADS 层加工完成！")


if __name__ == '__main__':
    run_dws_ads_load()