# -*- coding: utf-8 -*-
"""ClickHouse 查询工具（提取自 ai_layer/tools.py）"""
import re
import clickhouse_connect
from langchain_core.tools import tool

try:
    from src.common.config import cfg
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
    from config import cfg  # type: ignore[assignment]

try:
    from utils.retry import ch_retry
    from utils.logger import get_logger
except ImportError:
    from src.common.utils import get_logger
    def ch_retry(fn):
        return fn

log = get_logger('tools.clickhouse')


@ch_retry
def _get_ch():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


def _validate_sql(sql: str) -> str | None:
    upper = sql.strip().upper()
    for kw in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']:
        if re.search(rf'\b{kw}\b', upper):
            return f'错误：不允许执行 {kw} 操作'
    if not (upper.startswith('SELECT') or upper.startswith('WITH')):
        return '错误：只支持 SELECT 查询'
    return None


@tool
def query_data(sql: str) -> str:
    """
    在 ClickHouse 执行实时数据查询（仅支持 SELECT）。

    可用实时表：
    - ods.orders_stream        原始订单流（Kafka落地），字段：order_id/customer_id/product_category/price/freight_value/order_status/state/city/event_time
    - ods.payments_stream      原始支付流（Kafka落地），字段：payment_id/order_id/payment_type/payment_value/installments/event_time
    - dwd.realtime_order_detail  订单+支付宽表（Flink JOIN），字段：order_id/product_category/state/price/total_amount/payment_type/order_status/event_time/event_date/event_hour/is_paid
    - dws.realtime_minute_stats  分钟级聚合（Flink窗口），字段：window_start/window_end/order_cnt/total_gmv/avg_price/unique_customers/top_category
    - stream.ai_quality_alerts   AI异常告警，字段：alert_time/alert_type/severity/detail/ai_suggestion/metric_value
    - ads.realtime_hourly        今日小时趋势视图（已内置today()过滤）
    - ads.realtime_category_today  今日品类排行视图
    - ads.realtime_state_today     今日州排行视图

    注意：无历史批量表，查"最近N分钟"用 WHERE event_time >= now() - INTERVAL N MINUTE，查"今天"用 WHERE event_time >= today()。
    """
    err = _validate_sql(sql)
    if err:
        return err
    try:
        df = _get_ch().query_df(sql.strip().rstrip(';'))
        if df.empty:
            return '查询结果为空（该时间范围内暂无数据）'
        result = df.head(30).to_markdown(index=False)
        if len(df) > 30:
            result += f'\n\n（共 {len(df)} 行，显示前30行）'
        log.info('[query_data] 返回 %d 行', len(df))
        return result
    except Exception as e:
        log.error('[query_data] 失败：%s', e)
        return f'查询失败：{e}'


@tool
def get_table_schema(table_name: str) -> str:
    """
    查询指定 ClickHouse 表的结构（字段名、类型、注释）。
    table_name 格式：db.table，例如 dws.realtime_minute_stats
    """
    if '.' not in table_name:
        return '错误：table_name 格式应为 db.table'
    db, tbl = table_name.split('.', 1)
    try:
        rows = _get_ch().query(
            f"SELECT name, type, comment FROM system.columns "
            f"WHERE database = '{db}' AND table = '{tbl}' ORDER BY position"
        ).result_rows
        if not rows:
            return f'表 {table_name} 不存在或无字段信息'
        lines = [f'## {table_name} 表结构\n']
        for name, col_type, comment in rows:
            c = f'  # {comment}' if comment else ''
            lines.append(f'  {name}  {col_type}{c}')
        return '\n'.join(lines)
    except Exception as e:
        log.error('[get_table_schema] 失败：%s', e)
        return f'查询表结构失败：{e}'
