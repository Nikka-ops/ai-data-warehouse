# -*- coding: utf-8 -*-
import os, re, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.doris_client import get_client
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# 加载 .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')
CH_HOST     = os.getenv('DORIS_HOST', 'localhost')
CH_PORT     = int(os.getenv('DORIS_QUERY_PORT', '9030'))
CH_USER     = os.getenv('DORIS_USER', 'root')
CH_PASSWORD = os.getenv('DORIS_PASSWORD', '')

llm = OpenAI(api_key=DEEPSEEK_API_KEY, base_url='https://api.deepseek.com')

def get_ch_client():
    return get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )

TABLE_DESCRIPTIONS = {
    # ── DWD 明细层 ──
    'dwd.order_snapshot':       '订单累积快照，一个订单一行、记录其当前状态。'
                                '含 order_status（CREATED/PAID/SHIPPED/DELIVERED/CANCELED/REFUNDED）、'
                                '金额、各状态时间戳、pay_lag_seconds（下单到支付耗时）。'
                                '问「有多少订单处于某状态」「取消率」「支付耗时」只能查这张表',

    # ── DWS 汇总层（Flink 窗口聚合，秒级更新）──
    'dws.trade_window_agg':     '交易域窗口聚合。window_type=TUMBLE_1M 是分钟滚动窗口，'
                                'CUMULATE_1D 是当日累计窗口；category1_name 和 region_name '
                                '取值 ALL 表示汇总粒度。GMV 字段名是 order_amount',
    'dws.pay_window_agg':       '支付域窗口聚合，维度是 payment_type（ALL 表示汇总）。'
                                '含 pay_cnt/pay_user_cnt/pay_amount',
    'dws.user_active_daily':    '用户日活，uv_bitmap / pay_uv_bitmap 是 BITMAP 类型，'
                                '存当日下单/支付用户集合，可跨天精确合并；region_name=ALL 是全国',

    # ── ADS 应用层 ──
    'ads.category_rank':        '当日品类销售排行，含 gmv、rank_by_gmv、gmv_share_pct',
    'ads.region_rank':          '当日大区销售排行，含 gmv、uv（BITMAP 精确去重）、rank_by_gmv',
    'ads.reconcile_result':     '业务库与数仓对账结果，含 source_value/target_value/diff_pct/status',

    # ── 运维层 ──
    'stream.ai_quality_alerts': '实时质检告警表，记录规则命中的异常及 AI 给出的处置建议',
    'stream.late_records':      '迟到数据兜底表，超出 watermark 容忍度的记录落在这里，带 Kafka 位点',
    'stream.job_metrics':       'Flink 作业运行指标：背压、checkpoint 耗时、重启次数、消费积压',
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
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns "
            f"WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{tbl}' ORDER BY ORDINAL_POSITION"
        ).result_rows
        col_lines = [f"    {c[0]} {c[1]}" for c in cols if not c[0].startswith('_')]
        schema_parts.append(f"-- {desc}\n表名: {table}\n字段:\n" + "\n".join(col_lines))

    schema = "\n\n".join(schema_parts)
    SCHEMA_CACHE['schema'] = schema
    return schema

SYSTEM_PROMPT = """你是一位精通 Apache Doris SQL 的数据分析师。
根据下方的数据库表结构和业务规则，将用户的自然语言问题转换为可执行的 Doris SQL。

【SQL 方言】
Doris 使用 MySQL 兼容语法。以下 ClickHouse 写法在 Doris 上会直接报错，禁止使用：
- 禁止 count()      → 用 COUNT(*)
- 禁止 countIf(x)   → 用 SUM(CASE WHEN x THEN 1 ELSE 0 END)
- 禁止 argMax/argMin、toYYYYMM、parseDateTimeBestEffort、stddevPop 等 ClickHouse 专有函数
- 禁止 SELECT ... FINAL（Doris 的 Unique 表是写时合并，查询天然去重，不需要 FINAL）
- 日期用 MySQL 函数：DATE_FORMAT / DATE_SUB / DATE_ADD / CURDATE() / NOW()
- 取年月用 DATE_FORMAT(dt, '%Y-%m')

【业务背景】
电商实时数据仓库，金额单位为人民币（￥）。
数据由 Flink CDC 从 MySQL 业务库实时捕获，当天持续流入，没有历史批数据。
所以问「上个月」「去年」这类跨长周期的问题时，能查到的数据可能只有最近几天。

【业务规则】
- GMV 在 dws.trade_window_agg 里的字段名是 order_amount，不是 gmv
- 客单价 = order_amount / order_cnt
- 件均价 = order_amount / sku_num
- 订单状态：CREATED=已下单未支付, PAID=已支付, SHIPPED=已发货,
  DELIVERED=已送达, CANCELED=已取消, REFUNDED=已退款（全大写）
- 支付转化率 = 状态在 (PAID/SHIPPED/DELIVERED) 的订单数 / 总订单数
- 查 Top N 时用 ORDER BY xxx DESC LIMIT N

【UV / 独立用户数 —— 只能走 BITMAP】
dws.user_active_daily 的 uv_bitmap / pay_uv_bitmap 是 BITMAP 类型，不能直接 SELECT。
- 算 UV：BITMAP_UNION_COUNT(uv_bitmap)，跨天分区自动合并，结果精确
- 绝对不要写 COUNT(uv_bitmap) 或 SUM(uv_bitmap)
- 更不要把 dws.trade_window_agg 的 order_user_cnt 跨窗口 SUM 起来当 UV ——
  那是「单个窗口内的去重用户数」，跨窗口相加会把同一用户重复计数。
  凡是跨窗口、跨天的去重用户数，一律查 dws.user_active_daily。

【选表优先级】
同一个问题能用多层算出来时，一律选最高层，不要下钻到明细层自己聚合：
ADS > DWS > DWD。例如：
- 问「品类排名」「大区排名」→ 用 ads.category_rank / ads.region_rank，
  它们已经算好了 rank_by_gmv 和占比，不要去 dws 聚合再排序
- 问「今日累计 GMV」→ 用 dws.trade_window_agg 的 CUMULATE_1D 最新一行，
  不要把 TUMBLE_1M 的行 SUM 起来
- 问「链路延迟」→ 用 dws.trade_window_agg 的 max_lag_seconds / avg_lag_seconds

【只有 dwd.order_snapshot 能回答的问题】
凡是问「当前有多少订单处于某状态」「取消率」「支付转化率」「下单到支付多久」，
必须查 dwd.order_snapshot。dws 那两张窗口表装的是「发生了多少次事件」，
回答不了「现在是什么状态」—— 这是事务事实表和累积快照的根本区别。

【dws.trade_window_agg 的粒度陷阱 —— 最容易出错，务必逐条照做】
该表用 GROUPING SETS 一次写入了三种粒度，靠 category1_name / region_name 两列
取值 'ALL' 来区分汇总行与明细行。**两个维度列必须同时被约束**，
只约束一个会让另一个维度的汇总行与明细行叠加，结果偏大且不报错：

- 查整体指标：      WHERE category1_name = 'ALL' AND region_name = 'ALL'
- 按品类下钻：      WHERE category1_name <> 'ALL' AND region_name = 'ALL'
- 按大区下钻：      WHERE region_name <> 'ALL' AND category1_name = 'ALL'

错误示范（只约束了品类，region 的汇总行与明细行会叠加）：
  WHERE window_type='TUMBLE_1M' AND category1_name <> 'ALL'
正确写法：
  WHERE window_type='TUMBLE_1M' AND category1_name <> 'ALL' AND region_name = 'ALL'

另外 window_type 必须显式过滤，TUMBLE_1M 与 CUMULATE_1D 混在一起会重复计数。
dws.pay_window_agg 同理，维度列是 payment_type。

【数据库表结构】
{schema}

【输出要求】
1. 只返回 SQL 语句，不要任何解释文字
2. 不得包含 INSERT/UPDATE/DELETE/DROP 等写操作
3. SQL 末尾不要加分号
4. 数字结果用 ROUND() 保留2位小数
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
        "今天累计 GMV 和订单数是多少",
        "今天卖得最好的前 5 个一级品类",
        "哪个大区今天成交额最高？列出前 5 名",
        "今天的支付转化率是多少",
        "最近 7 天有多少独立下单用户",
        "最近一小时链路的最大端到端延迟是多少秒",
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
