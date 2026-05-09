# -*- coding: utf-8 -*-
import os, re, sys
import clickhouse_connect
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# 加载 .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')
CH_HOST     = os.getenv('CLICKHOUSE_HOST', 'localhost')
CH_PORT     = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_USER     = os.getenv('CLICKHOUSE_USER', 'admin')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')

llm = OpenAI(api_key=DEEPSEEK_API_KEY, base_url='https://api.deepseek.com')

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )

TABLE_DESCRIPTIONS = {
    'dws.order_daily':      '每日订单汇总表，包含每天的GMV、订单数、用户数、客单价等核心指标',
    'dws.category_daily':   '每日品类销售汇总表，可分析各商品品类的销售趋势',
    'ads.monthly_kpi':      '月度核心KPI表，包含月环比增长率，适合做月度趋势分析',
    'ads.state_sales_rank': '各省份每月销售排行榜，适合地域分析',
    'dwd.order_detail':     '订单明细宽表，包含每笔订单的完整信息，适合做精细化分析',
}

SCHEMA_CACHE = {}

def get_schema(client):
    global SCHEMA_CACHE
    if SCHEMA_CACHE:
        return SCHEMA_CACHE['schema']

    schema_parts = []
    for table, desc in TABLE_DESCRIPTIONS.items():
        db, tbl = table.split('.')
        cols = client.query(
            "SELECT name, type FROM system.columns "
            f"WHERE database='{db}' AND table='{tbl}' ORDER BY position"
        ).result_rows
        col_lines = [f"    {c[0]} {c[1]}" for c in cols if not c[0].startswith('_')]
        schema_parts.append(f"-- {desc}\n表名: {table}\n字段:\n" + "\n".join(col_lines))

    schema = "\n\n".join(schema_parts)
    SCHEMA_CACHE['schema'] = schema
    return schema

SYSTEM_PROMPT = """你是一位精通 ClickHouse SQL 的数据分析师。
根据下方的数据库表结构和业务规则，将用户的自然语言问题转换为可执行的 ClickHouse SQL。

【业务背景】
这是一个巴西电商平台的数据仓库，时间范围 2016年~2018年，金额单位为巴西雷亚尔(R$)。

【业务规则】
- dwd.order_detail 表中金额字段是 price（商品价格）和 freight_value（运费），没有 gmv 字段
- 查州/地域销售额时优先用 ads.state_sales_rank 表，不要查 dwd 层
- 订单状态：delivered=已送达, shipped=已发货, canceled=已取消
- GMV = 商品成交金额（不含运费）
- 客单价 = GMV / 订单数
- 分析趋势时优先使用 dws 或 ads 层，明细分析用 dwd 层
- 查 Top N 时用 ORDER BY xxx DESC LIMIT N

【数据库表结构】
{schema}

【输出要求】
1. 只返回 SQL 语句，不要任何解释文字
2. 不得包含 INSERT/UPDATE/DELETE/DROP 等写操作
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

def generate_sql(question, schema):
    prompt = SYSTEM_PROMPT.format(schema=schema)
    response = llm.chat.completions.create(
        model='deepseek-chat',
        messages=[
            {'role': 'system', 'content': prompt},
            {'role': 'user',   'content': question}
        ],
        temperature=0.1,
        max_tokens=1000,
    )
    sql = response.choices[0].message.content.strip()
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^```\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip().rstrip(';')

def generate_insight(question, sql, df):
    data_str = df.head(10).to_markdown(index=False)
    prompt = INSIGHT_PROMPT.format(question=question, sql=sql, data=data_str)
    response = llm.chat.completions.create(
        model='deepseek-chat',
        messages=[{'role': 'user', 'content': prompt}],
        temperature=0.7,
        max_tokens=500,
    )
    return response.choices[0].message.content.strip()

def validate_sql(sql):
    sql_upper = sql.strip().upper()
    for kw in ['INSERT','UPDATE','DELETE','DROP','CREATE','ALTER','TRUNCATE']:
        if re.search(rf'\b{kw}\b', sql_upper):
            raise ValueError(f"不允许执行 {kw} 操作")
    if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
        raise ValueError("SQL 必须以 SELECT 或 WITH 开头")

def nl2sql(question, with_insight=True):
    result = {'question': question, 'sql': '', 'data': pd.DataFrame(),
              'insight': '', 'row_count': 0, 'error': None}
    try:
        client = get_ch_client()
        schema = get_schema(client)

        print(f"[理解问题] {question}")
        sql = generate_sql(question, schema)
        result['sql'] = sql
        print(f"[生成SQL]\n{sql}\n")

        validate_sql(sql)

        print("[执行查询]")
        df = client.query_df(sql)
        result['data'] = df
        result['row_count'] = len(df)
        print(f"[查询完成] 返回 {len(df)} 行")

        if with_insight and len(df) > 0:
            print("[生成洞察]")
            insight = generate_insight(question, sql, df)
            result['insight'] = insight
            print(f"[洞察] {insight}")

    except Exception as e:
        result['error'] = str(e)
        print(f"[错误] {e}")

    return result


if __name__ == '__main__':
    test_questions = [
        "每个月的GMV是多少？按时间排序",
        "销售额最高的前5个商品品类是哪些？",
        "2018年各月的订单数和环比增长率",
        "哪个州的销售额最高？列出前10名",
        "每天平均有多少订单？最高峰是哪天？",
    ]

    print("=" * 60)
    print("  NL2SQL 测试")
    print("=" * 60)

    for i, q in enumerate(test_questions, 1):
        print(f"\n[问题 {i}] {q}")
        print("-" * 40)
        res = nl2sql(q, with_insight=False)
        if res['error']:
            print(f"[失败] {res['error']}")
        else:
            print(res['data'].head(5).to_string(index=False))
        print()
