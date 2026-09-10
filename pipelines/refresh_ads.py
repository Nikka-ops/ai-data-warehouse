# -*- coding: utf-8 -*-
"""
ADS 刷新任务

和常驻流作业的分工是刻意划开的：

    常驻流作业（Flink）   负责「事件一发生就入仓」—— DWD 快照、DWS 窗口，
                          秒级、不停机、靠 checkpoint 自愈。
    本任务（独立调度）     负责「算起来重、但不需要秒级新鲜度」的那部分 ——
                          跨天 BITMAP 去重、排行榜。

分开的理由不是洁癖，是故障隔离：这个脚本跑挂了（LLM 超时、Doris 慢查询、
调度器自己挂了），实时链路照常写入，看板只是排行榜旧了几分钟；
反过来它也绝不会因为占用 Flink 的 slot 或状态而拖累主链路。
把它塞进 Flink 作业里就没有这条边界了。

刷三样东西：
    dws.user_active_daily   BITMAP 日活（跨天精确去重的底座）
    ads.category_rank       品类排行
    ads.region_rank         地域排行

运行：
    python pipelines/refresh_ads.py                # 刷今天
    python pipelines/refresh_ads.py --date 2026-09-08
    python pipelines/refresh_ads.py --days 7       # 回刷最近 7 天
"""

import os
import sys
import argparse
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common.doris_client import get_client


# ============================================================
# 一、BITMAP 日活
#
# 这里有个必须处理的幂等问题，也是 Aggregate 模型最容易踩的坑：
#
# dws.user_active_daily 是 AGGREGATE KEY + SUM/BITMAP_UNION 模型，
# 同一个 key 再插一次，SUM 列会**累加**而不是覆盖。
# 也就是说这个脚本重跑一次，order_cnt 就翻一倍。
# （BITMAP 列反而没事 —— 位图求并是幂等的，同一批用户并多少次都一样。）
#
# 所以刷新前必须先按分区键删掉当天数据，「先清后写」整体才幂等。
# 这正是 DWS 层选型注释里那条规则的另一面：
# Flink 直写的表用 Unique（写入端天然幂等），
# 调度刷新的表用 Aggregate（幂等性由刷新任务自己保证）。
# ============================================================

SQL_DELETE_UV = """
DELETE FROM dws.user_active_daily WHERE dt = %s
"""

# 分大区粒度
SQL_UV_BY_REGION = """
INSERT INTO dws.user_active_daily
    (dt, region_name, order_cnt, order_amount, uv_bitmap, pay_uv_bitmap)
SELECT
    create_date,
    COALESCE(region_name, '未知'),
    COUNT(*),
    SUM(total_amount),
    -- user_id 本身是 BIGINT，直接 TO_BITMAP 即可；
    -- 只有字符串主键才需要先 BITMAP_HASH 映射成整型（会引入极小的碰撞概率）
    BITMAP_UNION(TO_BITMAP(user_id)),
    BITMAP_UNION(
        CASE WHEN order_status IN ('PAID', 'SHIPPED', 'DELIVERED')
             THEN TO_BITMAP(user_id)
             ELSE BITMAP_EMPTY() END
    )
FROM dwd.order_snapshot
WHERE create_date = %s
GROUP BY create_date, COALESCE(region_name, '未知')
"""

# 全国粒度单独存一行：
# 各大区的 uv_bitmap 直接 BITMAP_UNION_COUNT 合并也能得到全国 UV，
# 但那要读全部大区的位图。全国 UV 是看板首屏必查的数，
# 单独存一行换一次点查，值得这点冗余。
SQL_UV_ALL = """
INSERT INTO dws.user_active_daily
    (dt, region_name, order_cnt, order_amount, uv_bitmap, pay_uv_bitmap)
SELECT
    create_date,
    'ALL',
    COUNT(*),
    SUM(total_amount),
    BITMAP_UNION(TO_BITMAP(user_id)),
    BITMAP_UNION(
        CASE WHEN order_status IN ('PAID', 'SHIPPED', 'DELIVERED')
             THEN TO_BITMAP(user_id)
             ELSE BITMAP_EMPTY() END
    )
FROM dwd.order_snapshot
WHERE create_date = %s
GROUP BY create_date
"""


# ============================================================
# 二、品类排行
#
# 数据源是 DWS 的分钟窗口，不是 DWD 明细 —— 明细里没有品类
# （品类挂在订单明细的 SKU 上，而快照表是订单粒度）。
# 从已经聚合好的分钟窗口再滚一层，扫描量是明细的几百分之一。
#
# 注意 WHERE 里两个维度列都约束了：category1_name <> 'ALL' 取品类明细行，
# region_name = 'ALL' 排除掉大区粒度的那批行。少写一个条件，
# 品类行会和大区行叠加，GMV 直接翻倍且不报错 —— GROUPING SETS 的经典陷阱。
# ============================================================

SQL_CATEGORY_RANK = """
INSERT INTO ads.category_rank
    (stat_date, category1_name, order_cnt, sku_num, gmv,
     avg_price, rank_by_gmv, gmv_share_pct, refresh_time)
SELECT
    stat_date,
    category1_name,
    order_cnt,
    sku_num,
    gmv,
    CASE WHEN sku_num > 0 THEN ROUND(gmv / sku_num, 2) ELSE 0 END,
    ROW_NUMBER() OVER (ORDER BY gmv DESC),
    CASE WHEN SUM(gmv) OVER () > 0
         THEN ROUND(gmv * 100.0 / SUM(gmv) OVER (), 2)
         ELSE 0 END,
    NOW()
FROM (
    SELECT
        stat_date,
        category1_name,
        SUM(order_cnt)                          AS order_cnt,
        SUM(sku_num)                            AS sku_num,
        SUM(order_amount)                       AS gmv
    FROM dws.trade_window_agg
    WHERE stat_date       = %s
      AND window_type     = 'TUMBLE_1M'
      AND category1_name <> 'ALL'
      AND region_name     = 'ALL'
    GROUP BY stat_date, category1_name
) t
"""


# ============================================================
# 三、地域排行
#
# GMV / 订单数来自 DWS 窗口，UV 来自 BITMAP 表 —— 两个不同的源。
# 之所以不把 UV 也从窗口表取：窗口表里的 order_user_cnt 是
# 「那一分钟内的去重用户数」，跨窗口 SUM 起来会把同一个用户重复计数。
# 全天去重只能走 BITMAP，这就是那张表存在的理由。
#
# 所以本任务里 BITMAP 必须先于排行刷新 —— 顺序不能颠倒。
# ============================================================

SQL_REGION_RANK = """
INSERT INTO ads.region_rank
    (stat_date, region_name, order_cnt, gmv, uv, pay_uv, rank_by_gmv, refresh_time)
SELECT
    w.stat_date,
    w.region_name,
    w.order_cnt,
    w.gmv,
    COALESCE(u.uv, 0),
    COALESCE(u.pay_uv, 0),
    ROW_NUMBER() OVER (ORDER BY w.gmv DESC),
    NOW()
FROM (
    SELECT
        stat_date,
        region_name,
        SUM(order_cnt)                          AS order_cnt,
        SUM(order_amount)                       AS gmv
    FROM dws.trade_window_agg
    WHERE stat_date       = %s
      AND window_type     = 'TUMBLE_1M'
      AND region_name    <> 'ALL'
      AND category1_name  = 'ALL'
    GROUP BY stat_date, region_name
) w
LEFT JOIN (
    SELECT
        dt,
        region_name,
        BITMAP_UNION_COUNT(uv_bitmap)           AS uv,
        BITMAP_UNION_COUNT(pay_uv_bitmap)       AS pay_uv
    FROM dws.user_active_daily
    WHERE dt = %s AND region_name <> 'ALL'
    GROUP BY dt, region_name
) u ON w.region_name = u.region_name
"""


def refresh_one_day(doris, day: date) -> dict:
    """刷新单日的 ADS。返回各步骤影响行数。"""
    d = day.isoformat()
    stats = {}

    # 1. BITMAP 日活：先清后写，保证重跑幂等
    doris.command(SQL_DELETE_UV, (d,))
    stats['uv_by_region'] = doris.command(SQL_UV_BY_REGION, (d,))
    stats['uv_all']       = doris.command(SQL_UV_ALL, (d,))

    # 2. 排行表是 Unique Key 模型，按主键覆盖，重跑天然幂等，不需要先删
    stats['category_rank'] = doris.command(SQL_CATEGORY_RANK, (d,))
    stats['region_rank']   = doris.command(SQL_REGION_RANK, (d, d))

    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description='刷新 ADS 层与 BITMAP 日活')
    ap.add_argument('--date', help='刷新指定日期，格式 YYYY-MM-DD，默认今天')
    ap.add_argument('--days', type=int, default=1,
                    help='从 --date 往前回刷 N 天，默认 1')
    args = ap.parse_args()

    end = (datetime.strptime(args.date, '%Y-%m-%d').date()
           if args.date else date.today())

    doris = get_client()
    if not doris.ping():
        print('[ADS刷新] Doris 不可达，退出', file=sys.stderr)
        return 1

    failed = 0
    for i in range(args.days):
        day = end - timedelta(days=i)
        try:
            stats = refresh_one_day(doris, day)
            detail = '  '.join(f'{k}={v}' for k, v in stats.items())
            print(f'[ADS刷新] {day}  {detail}')
        except Exception as e:
            failed += 1
            print(f'[ADS刷新] {day} 失败：{e}', file=sys.stderr)

    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
