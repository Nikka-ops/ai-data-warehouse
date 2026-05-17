# -*- coding: utf-8 -*-
import os, re, sys, time
import clickhouse_connect
import pandas as pd
from openai import OpenAI

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import llm_retry, ch_retry

log = get_logger('nl2sql')

# ── LLM 客户端 ────────────────────────────────────────────────
llm = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url)

# ── ClickHouse 连接 ───────────────────────────────────────────
@ch_retry
def get_ch_client():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=120,
    )

# ── 表描述（批量历史 + 实时流）────────────────────────────────
TABLE_DESCRIPTIONS = {
    # 批量历史数据层（2016-2018）
    'dws.order_daily':          '每日订单汇总表，包含每天GMV、订单数、用户数、客单价等核心指标（历史数据2016-2018）',
    'dws.category_daily':       '每日品类销售汇总表，可分析各商品品类的销售趋势（历史数据）',
    'ads.monthly_kpi':          '月度核心KPI表，含月环比增长率，适合做月度趋势分析（历史数据）',
    'ads.state_sales_rank':     '各省份每月销售排行榜，适合地域分析（历史数据）',
    'dwd.order_detail':         '订单明细宽表，包含每笔订单完整信息，适合精细化分析（历史数据）',
    # 实时流数据层（近24小时）
    'ods.orders_stream':        '实时订单流ODS落地表，包含近期实时订单（近24小时），字段：order_id/customer_id/product_category/price/freight_value/order_status/state/city/event_time',
    'dwd.realtime_order_detail':'实时订单+支付宽表，已关联支付信息，字段：order_id/product_category/state/price/payment_type/order_status/event_time/event_date',
    'dws.realtime_minute_stats':'实时分钟级聚合统计（Flink输出），字段：window_start/window_end/order_cnt/total_gmv/avg_price/unique_customers/top_category',
    'stream.ai_quality_alerts': '实时AI异常告警表，字段：alert_time/alert_type/severity/detail/ai_suggestion/window_start/window_end',
}

# ── Schema 缓存（带TTL）───────────────────────────────────────
_schema_cache: dict = {}

def _cache_expired() -> bool:
    ts = _schema_cache.get('_ts', 0)
    return (time.time() - ts) > cfg.schema_cache_ttl

@ch_retry
def get_schema(client=None) -> str:
    global _schema_cache
    if _schema_cache.get('schema') and not _cache_expired():
        return _schema_cache['schema']

    if client is None:
        client = get_ch_client()

    log.info('刷新 Schema 缓存...')
    parts = []
    for table, desc in TABLE_DESCRIPTIONS.items():
        db, tbl = table.split('.')
        try:
            cols = client.query(
                "SELECT name, type FROM system.columns "
                f"WHERE database='{db}' AND table='{tbl}' ORDER BY position"
            ).result_rows
            col_lines = [f"    {c[0]} {c[1]}" for c in cols if not c[0].startswith('_')]
            parts.append(f"-- {desc}\n表名: {table}\n字段:\n" + "\n".join(col_lines))
        except Exception as e:
            log.warning('无法获取表 %s 的 Schema：%s', table, e)

    schema = "\n\n".join(parts)
    _schema_cache = {'schema': schema, '_ts': time.time()}
    log.info('Schema 缓存已更新，包含 %d 张表', len(parts))
    return schema

def invalidate_schema_cache():
    _schema_cache.clear()

# ── Prompt 模板 ───────────────────────────────────────────────
SYSTEM_PROMPT = """你是一位精通 ClickHouse SQL 的数据分析师。
根据下方的数据库表结构和业务规则，将用户的自然语言问题转换为可执行的 ClickHouse SQL。

【业务背景】
这是一个巴西电商平台的数据仓库。
- 历史批量数据：时间范围 2016年~2018年，金额单位为巴西雷亚尔(R$)
- 实时流数据：ods.orders_stream / dwd.realtime_order_detail / dws.realtime_minute_stats（近24小时）

【业务规则】
- dwd.order_detail / ods.orders_stream 中用 price 表示商品价格，没有 gmv 字段
- 查"实时"/"当前"/"今天"数据 → 优先用 ods.orders_stream 或 dwd.realtime_order_detail
- 查"趋势"/"历史"/"月度" → 用 dws/ads 历史层
- 查州/地域历史销售额 → 用 ads.state_sales_rank
- 查实时告警 → 用 stream.ai_quality_alerts
- 查分钟级实时流量 → 用 dws.realtime_minute_stats
- GMV = 商品成交金额（不含运费）
- 客单价 = GMV / 订单数
- Top N 用 ORDER BY xxx DESC LIMIT N

【数据库表结构】
{schema}

【输出要求】
1. 只返回 SQL 语句，不要任何解释文字
2. 不得包含 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE 等写操作
3. SQL 末尾不要加分号
4. 数字结果用 round() 保留2位小数
"""

INSIGHT_PROMPT = """你是一位数据分析师，请根据以下查询结果给出简洁的业务洞察（3-5句话）。
用户问题：{question}
执行的SQL：{sql}
查询结果（前10行）：
{data}
要求：直接给出洞察结论，指出最重要的数字和趋势，语言简洁专业，使用中文。
"""

# ── SQL 生成 ──────────────────────────────────────────────────
@llm_retry
def generate_sql(question: str, schema: str) -> str:
    prompt = SYSTEM_PROMPT.format(schema=schema)
    response = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=[
            {'role': 'system', 'content': prompt},
            {'role': 'user',   'content': question},
        ],
        temperature=cfg.nl2sql_temperature,
        max_tokens=1000,
    )
    sql = response.choices[0].message.content.strip()
    # 去掉 markdown 代码块
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^```\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip().rstrip(';')

@llm_retry
def generate_insight(question: str, sql: str, df: pd.DataFrame) -> str:
    data_str = df.head(10).to_markdown(index=False)
    prompt = INSIGHT_PROMPT.format(question=question, sql=sql, data=data_str)
    response = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': prompt}],
        temperature=cfg.insight_temperature,
        max_tokens=500,
    )
    return response.choices[0].message.content.strip()

def validate_sql(sql: str):
    sql_upper = sql.strip().upper()
    for kw in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']:
        if re.search(rf'\b{kw}\b', sql_upper):
            raise ValueError(f'不允许执行 {kw} 操作')
    if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
        raise ValueError('SQL 必须以 SELECT 或 WITH 开头')

# ── 主入口 ────────────────────────────────────────────────────
def nl2sql(question: str, with_insight: bool = True) -> dict:
    result = {
        'question': question, 'sql': '', 'data': pd.DataFrame(),
        'insight': '', 'row_count': 0, 'error': None,
    }
    try:
        client = get_ch_client()
        schema = get_schema(client)

        log.info('[理解问题] %s', question)
        sql = generate_sql(question, schema)
        result['sql'] = sql
        log.info('[生成SQL]\n%s', sql)

        validate_sql(sql)

        log.info('[执行查询]')
        df = client.query_df(sql)
        result['data'] = df
        result['row_count'] = len(df)
        log.info('[查询完成] 返回 %d 行', len(df))

        if with_insight and len(df) > 0:
            log.info('[生成洞察]')
            insight = generate_insight(question, sql, df)
            result['insight'] = insight

    except Exception as e:
        result['error'] = str(e)
        log.error('[错误] %s', e)

    return result


if __name__ == '__main__':
    test_questions = [
        "每个月的GMV是多少？按时间排序",
        "实时订单量最近1小时的趋势是怎样的？",
        "今天各品类的实时销售额排行",
    ]
    for q in test_questions:
        print(f"\n[问题] {q}")
        res = nl2sql(q, with_insight=False)
        if res['error']:
            print(f"[失败] {res['error']}")
        else:
            print(res['data'].head(5).to_string(index=False))
