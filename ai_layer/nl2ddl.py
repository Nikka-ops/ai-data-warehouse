# -*- coding: utf-8 -*-
"""
NL2DDL：自然语言 → ClickHouse CREATE VIEW
用户描述分析需求，AI 生成视图 DDL，确认后执行并注册到 stream.custom_views。
"""

import os
import sys
import re
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import llm_retry, ch_retry

log = get_logger('nl2ddl')

# 只允许创建 ads.* 或 dws.* 下的视图，禁止修改系统表
ALLOWED_PREFIXES  = ('ads.', 'dws.')
FORBIDDEN_PATTERN = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE\s+TABLE|CREATE\s+DATABASE)\b',
    re.IGNORECASE,
)

_DDL_SYSTEM_PROMPT = """你是 ClickHouse SQL 专家，负责根据用户的业务分析需求生成 CREATE VIEW 语句。

【可用实时表】
- ods.orders_stream       原始订单流（price/freight_value/product_category/state/order_status/event_time/customer_id/seller_id）
- ods.payments_stream     原始支付流（payment_type/payment_value/installments/event_time）
- dwd.realtime_order_detail  订单+支付宽表（已 JOIN，含 total_amount/is_paid/event_hour）
- dws.realtime_minute_stats  分钟级聚合（order_cnt/total_gmv/avg_price/unique_customers/top_category）
- stream.ai_quality_alerts   AI 告警（severity/detail/metric_value）
- ads.realtime_hourly        今日小时视图（内置 today() 过滤）
- ads.realtime_category_today  今日品类视图
- ads.realtime_state_today     今日州排行视图

【命名规则】
- 视图名必须以 ads. 或 dws. 开头
- 名称小写下划线，清晰描述业务含义
- 示例：ads.seller_hourly_gmv / ads.category_cancel_rate / dws.realtime_seller_stats

【SQL 规范】
- 使用 CREATE VIEW IF NOT EXISTS <视图名> AS SELECT ...
- 时间维度使用 toStartOfHour/toStartOfMinute 等函数
- 数值保留2位小数用 round()
- 聚合视图加 GROUP BY
- 不要加 ORDER BY（视图中无意义）
- 不要加时间过滤（由查询时决定），除非是专门的"今日"视图

【输出格式】
只输出可直接执行的 SQL，不加任何解释，不加 markdown 代码块标记。
"""


@llm_retry
def _generate_ddl(description: str) -> str:
    from openai import OpenAI
    client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=30.0)
    resp = client.chat.completions.create(
        model=cfg.llm_model,
        messages=[
            {'role': 'system', 'content': _DDL_SYSTEM_PROMPT},
            {'role': 'user',   'content': description},
        ],
        temperature=0.1,
        max_tokens=600,
    )
    sql = resp.choices[0].message.content.strip()
    # 去掉 markdown 代码块
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^```\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip()


def _validate_ddl(sql: str) -> str | None:
    """返回错误原因，None 表示通过"""
    upper = sql.strip().upper()

    if FORBIDDEN_PATTERN.search(sql):
        return '包含禁止操作（仅允许 CREATE VIEW）'

    if not re.match(r'CREATE\s+(VIEW|MATERIALIZED\s+VIEW)', upper):
        return 'DDL 必须以 CREATE VIEW 开头'

    # 提取视图名，检查是否在允许的前缀下
    match = re.search(
        r'CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)',
        sql, re.IGNORECASE
    )
    if not match:
        return '无法解析视图名称'

    view_name = match.group(1).strip('`"').lower()
    if not any(view_name.startswith(p) for p in ALLOWED_PREFIXES):
        return f'视图名必须以 {" 或 ".join(ALLOWED_PREFIXES)} 开头，当前：{view_name}'

    return None


@ch_retry
def _execute_ddl(sql: str) -> str:
    """执行 DDL 并注册到 stream.custom_views"""
    import clickhouse_connect
    ch = clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
    )
    ch.command(sql)

    # 提取视图名
    match = re.search(
        r'CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)',
        sql, re.IGNORECASE
    )
    view_name = match.group(1).strip('`"') if match else 'unknown'

    ch.insert(
        'stream.custom_views',
        [[str(uuid.uuid4()), view_name, '', sql, 'nl2ddl', datetime.now()]],
        column_names=['view_id', 'view_name', 'description', 'ddl_sql', 'created_by', 'created_at'],
    )
    return view_name


def nl2ddl(description: str) -> dict:
    """
    将自然语言描述转换为 CREATE VIEW DDL 并执行。
    返回：{'view_name', 'ddl', 'error'}
    """
    result = {'description': description, 'ddl': '', 'view_name': '', 'error': None}
    try:
        log.info('[NL2DDL] %s', description[:80])
        ddl = _generate_ddl(description)
        result['ddl'] = ddl
        log.info('[生成DDL]\n%s', ddl)

        err = _validate_ddl(ddl)
        if err:
            result['error'] = f'DDL 校验失败：{err}'
            return result

        view_name = _execute_ddl(ddl)
        result['view_name'] = view_name
        log.info('[NL2DDL] 视图 %s 创建成功', view_name)

    except Exception as e:
        result['error'] = str(e)
        log.error('[NL2DDL错误] %s', e)

    return result


def list_custom_views() -> list[dict]:
    """查询已创建的自定义视图列表"""
    try:
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port,
            username=cfg.ch_user, password=cfg.ch_password,
        )
        rows = ch.query("""
            SELECT view_name, description, created_at
            FROM stream.custom_views
            ORDER BY created_at DESC LIMIT 20
        """).result_rows
        return [{'view_name': r[0], 'description': r[1], 'created_at': r[2]} for r in rows]
    except Exception as e:
        log.warning('获取自定义视图列表失败：%s', e)
        return []
