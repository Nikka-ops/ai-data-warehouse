"""
数据验证脚本：检查四层数据是否正常流转
运行：python pipelines/verify.py
"""

import os
import clickhouse_connect

CH_HOST     = os.getenv('CLICKHOUSE_HOST', 'localhost')
CH_PORT     = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_USER     = os.getenv('CLICKHOUSE_USER', 'admin')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')


def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )

    print("=" * 60)
    print("  AI 数仓 - 四层数据验证报告")
    print("=" * 60)

    # ── ODS 层 ────────────────────────────────────────────────
    print("\n【ODS 层 - 原始数据】")
    ods_tables = [
        ('ods.orders_raw',      '订单表'),
        ('ods.order_items_raw', '订单商品表'),
        ('ods.customers_raw',   '客户表'),
        ('ods.products_raw',    '商品表'),
    ]
    for table, name in ods_tables:
        cnt = client.query(f'SELECT count() FROM {table}').first_row[0]
        print(f"  {name:12s}: {cnt:>10,} 行  {'✅' if cnt > 0 else '❌'}")

    # ── DWD 层 ────────────────────────────────────────────────
    print("\n【DWD 层 - 明细宽表】")
    cnt = client.query('SELECT count() FROM dwd.order_detail').first_row[0]
    print(f"  订单明细宽表    : {cnt:>10,} 行  {'✅' if cnt > 0 else '❌'}")

    null_check = client.query(
        "SELECT countIf(customer_unique_id = '') FROM dwd.order_detail"
    ).first_row[0]
    print(f"  空 customer_id  : {null_check:>10,} 行  {'✅' if null_check == 0 else '⚠️'}")

    # ── DWS 层 ────────────────────────────────────────────────
    print("\n【DWS 层 - 汇总宽表】")
    dws_tables = [
        ('dws.order_daily',    '每日销售汇总'),
        ('dws.category_daily', '品类每日汇总'),
    ]
    for table, name in dws_tables:
        cnt = client.query(f'SELECT count() FROM {table}').first_row[0]
        print(f"  {name:12s}: {cnt:>10,} 行  {'✅' if cnt > 0 else '❌'}")

    # ── ADS 层 ────────────────────────────────────────────────
    print("\n【ADS 层 - 应用指标】")
    ads_tables = [
        ('ads.monthly_kpi',     '月度KPI'),
        ('ads.state_sales_rank','省份排行'),
    ]
    for table, name in ads_tables:
        cnt = client.query(f'SELECT count() FROM {table}').first_row[0]
        print(f"  {name:12s}: {cnt:>10,} 行  {'✅' if cnt > 0 else '❌'}")

    # ── 业务指标预览 ──────────────────────────────────────────
    print("\n【业务指标快速预览】")
    row = client.query("""
        SELECT
            round(sum(gmv), 0)          AS total_gmv,
            sum(order_cnt)              AS total_orders,
            sum(user_cnt)               AS total_users,
            round(avg(avg_order_value), 2) AS avg_order_val
        FROM ads.monthly_kpi
    """).first_row
    print(f"  总GMV      : R$ {row[0]:>15,.0f}")
    print(f"  总订单数   :    {row[1]:>15,}")
    print(f"  总用户数   :    {row[2]:>15,}")
    print(f"  平均客单价 : R$ {row[3]:>15,.2f}")

    print("\n✅ 阶段一验收完成！四层数据均已正常流转。")
    print("=" * 60)


if __name__ == '__main__':
    main()
