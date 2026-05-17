# -*- coding: utf-8 -*-
"""ETL：ODS → DWD（幂等：INSERT + OPTIMIZE FINAL 替代 TRUNCATE）"""
import os, sys
import clickhouse_connect
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('etl_dwd')


@ch_retry
def get_client():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
    )


# INSERT 使用 now() 作为 _load_time 版本号
# ReplacingMergeTree 每次重跑会保留最新版本，天然幂等
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
    replaceAll(
        initcap(replaceAll(coalesce(p.product_category_name, 'unknown'), '_', ' ')),
        ' ', '_'
    )                                                       AS product_category,
    oi.seller_id,
    o.order_status,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value                            AS total_amount,
    toDate(o.order_purchase_ts)                            AS order_date,
    toYear(o.order_purchase_ts)                            AS order_year,
    toMonth(o.order_purchase_ts)                           AS order_month,
    toHour(o.order_purchase_ts)                            AS order_hour,
    if(
        isNotNull(o.order_delivered_ts),
        dateDiff('day', o.order_purchase_ts, o.order_delivered_ts),
        NULL
    )                                                       AS delivery_days,
    if(o.order_status = 'delivered', 1, 0)                AS is_delivered,
    now()                                                   AS _load_time
FROM ods.order_items_raw  oi
LEFT JOIN ods.orders_raw      o ON oi.order_id    = o.order_id
LEFT JOIN ods.customers_raw   c ON o.customer_id  = c.customer_id
LEFT JOIN ods.products_raw    p ON oi.product_id  = p.product_id
WHERE isNotNull(o.order_purchase_ts)
  AND o.order_purchase_ts >= '2016-01-01'
"""


def run_dwd_load():
    log.info('========== DWD 层加工开始 ==========')
    start = datetime.now()
    client = get_client()
    log.info('ClickHouse 连接成功')

    log.info('执行 ODS → DWD 关联加工（幂等 INSERT，无 TRUNCATE）...')
    client.command(DWD_SQL)

    log.info('触发 ReplacingMergeTree 去重合并...')
    client.command('OPTIMIZE TABLE dwd.order_detail FINAL')

    cnt = client.query('SELECT count() FROM dwd.order_detail').first_row[0]
    log.info('dwd.order_detail 共 %d 行', cnt)

    # 数据质量校验
    null_city = client.query(
        "SELECT countIf(city = '') FROM dwd.order_detail"
    ).first_row[0]
    log.info('空城市字段：%d 行', null_city)

    status_dist = client.query("""
        SELECT order_status, count() AS cnt
        FROM dwd.order_detail
        GROUP BY order_status ORDER BY cnt DESC
    """).result_rows
    log.info('订单状态分布：')
    for row in status_dist:
        log.info('  %s: %d', row[0], row[1])

    elapsed = (datetime.now() - start).total_seconds()
    log.info('========== DWD 层加工完成（耗时 %.1f 秒）==========', elapsed)


if __name__ == '__main__':
    run_dwd_load()
