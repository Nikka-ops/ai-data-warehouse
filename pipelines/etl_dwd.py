"""
ETL 脚本：ODS → DWD
将四张 ODS 原始表关联清洗，生成订单明细宽表 dwd.order_detail
"""

import os
import clickhouse_connect
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


DWD_SQL = """
INSERT INTO dwd.order_detail
SELECT
    oi.order_id,
    oi.order_item_id,
    o.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    oi.product_id,
    -- 品类名标准化：下划线转空格，首字母大写
    replaceAll(
        initcap(replaceAll(coalesce(p.product_category_name, 'unknown'), '_', ' ')),
        ' ', '_'
    )                                                       AS product_category,
    oi.seller_id,
    o.order_status,
    -- 金额
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value                            AS total_amount,
    -- 时间衍生
    toDate(o.order_purchase_ts)                            AS order_date,
    toYear(o.order_purchase_ts)                            AS order_year,
    toMonth(o.order_purchase_ts)                           AS order_month,
    toHour(o.order_purchase_ts)                            AS order_hour,
    -- 配送天数（下单到送达）
    if(
        isNotNull(o.order_delivered_ts),
        dateDiff('day', o.order_purchase_ts, o.order_delivered_ts),
        NULL
    )                                                       AS delivery_days,
    -- 是否已送达
    if(o.order_status = 'delivered', 1, 0)                AS is_delivered,
    now()                                                   AS _load_time

FROM ods.order_items_raw  oi
LEFT JOIN ods.orders_raw      o  ON oi.order_id    = o.order_id
LEFT JOIN ods.customers_raw   c  ON o.customer_id  = c.customer_id
LEFT JOIN ods.products_raw    p  ON oi.product_id  = p.product_id

-- 过滤脏数据：必须有下单时间
WHERE isNotNull(o.order_purchase_ts)
  AND o.order_purchase_ts >= '2016-01-01'
"""


def run_dwd_load():
    print("=" * 55)
    print("  AI 数仓 - DWD 层加工")
    print(f"  时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    client = get_client()
    print("✅ ClickHouse 连接成功")

    print("\n🔄 清空旧数据...")
    client.command("TRUNCATE TABLE dwd.order_detail")

    print("🔄 执行 ODS → DWD 关联加工...")
    client.command(DWD_SQL)

    cnt = client.query("SELECT count() FROM dwd.order_detail").first_row[0]
    print(f"✅ DWD 加工完成，dwd.order_detail 共 {cnt:,} 行")

    # 简单数据质量校验
    print("\n📊 数据质量检查：")

    null_city = client.query(
        "SELECT countIf(city = '') FROM dwd.order_detail"
    ).first_row[0]
    print(f"  空城市字段：{null_city:,} 行")

    status_dist = client.query("""
        SELECT order_status, count() as cnt
        FROM dwd.order_detail
        GROUP BY order_status
        ORDER BY cnt DESC
    """).result_rows
    print("  订单状态分布：")
    for row in status_dist:
        print(f"    {row[0]:15s}: {row[1]:>8,}")

    print("\n🎉 DWD 层加工完成！")


if __name__ == '__main__':
    run_dwd_load()
