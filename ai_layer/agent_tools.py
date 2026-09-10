# -*- coding: utf-8 -*-
"""
Agent 工具集
定义 Agent 可以调用的所有工具：数据查询、知识检索、洞察生成、报告输出
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import re
import json
from datetime import datetime
from common.doris_client import get_client
import pandas as pd
from openai import OpenAI
from langchain.tools import tool

CH_HOST     = os.getenv('DORIS_HOST', 'localhost')
CH_PORT     = int(os.getenv('DORIS_QUERY_PORT', '9030'))
CH_USER     = os.getenv('DORIS_USER', 'root')
CH_PASSWORD = os.getenv('DORIS_PASSWORD', '')


def get_ch():
    return get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )

def get_llm():
    return OpenAI(
        api_key=os.getenv('DEEPSEEK_API_KEY', ''),
        base_url='https://api.deepseek.com',
        timeout=60.0
    )


# ── 工具1：执行 SQL 查询 ──────────────────────────────────────
@tool
def query_data(sql: str) -> str:
    """
    在 Doris 数仓执行 SQL 查询，返回结果。
    输入：合法的 SELECT SQL 语句。
    输出：查询结果的 Markdown 表格字符串。
    注意：只支持 SELECT 查询，不支持写操作。
    """
    try:
        # 安全校验
        sql_upper = sql.strip().upper()
        for kw in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']:
            if re.search(rf'\b{kw}\b', sql_upper):
                return f"错误：不允许执行 {kw} 操作"
        if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
            return "错误：SQL 必须以 SELECT 或 WITH 开头"

        ch = get_ch()
        df = ch.query_df(sql.strip().rstrip(';'))

        if len(df) == 0:
            return "查询结果为空"

        # 限制返回行数避免 token 超限
        result = df.head(50).to_markdown(index=False)
        if len(df) > 50:
            result += f"\n\n（共 {len(df)} 行，只显示前50行）"
        return result

    except Exception as e:
        return f"查询失败：{str(e)}"


# ── 工具2：自然语言转 SQL 并执行 ─────────────────────────────
@tool
def nl_query(question: str) -> str:
    """
    用自然语言描述查询需求，自动生成 SQL 并执行，返回结果。
    适合不确定具体 SQL 写法时使用。
    输入：自然语言问题，如"查询每月GMV趋势"。
    输出：查询结果的 Markdown 表格。
    """
    try:
        ch = get_ch()
        llm = get_llm()

        # 获取 schema
        # 与 ai_layer/nl2sql.py 的 TABLE_DESCRIPTIONS 保持同一份认知，
        # 避免 Agent 和 NL2SQL 对同一个库有两套说法
        tables = {
            'dwd.order_snapshot':    '订单累积快照，一单一行记录当前状态。含 create_date、order_id、'
                                     'order_status、total_amount、region_name、pay_lag_seconds',
            'dws.trade_window_agg':  '交易域窗口聚合，GMV 字段名是 order_amount。'
                                     'window_type=TUMBLE_1M/CUMULATE_1D；'
                                     'category1_name 与 region_name 取 ALL 表示汇总粒度',
            'dws.pay_window_agg':    '支付域窗口聚合，含 pay_cnt、pay_amount，维度 payment_type',
            'dws.user_active_daily': '用户日活，uv_bitmap 为 BITMAP 类型，跨天去重用 BITMAP_UNION_COUNT',
            'ads.category_rank':     '当日品类排行，含 category1_name、gmv、rank_by_gmv、gmv_share_pct',
            'ads.region_rank':       '当日大区排行，含 region_name、gmv、uv、rank_by_gmv',
        }
        schema_parts = []
        for table, desc in tables.items():
            db, tbl = table.split('.')
            cols = ch.query(
                f"SELECT COLUMN_NAME,DATA_TYPE FROM information_schema.columns "
                f"WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{tbl}' ORDER BY ORDINAL_POSITION"
            ).result_rows
            col_lines = [f"  {c[0]} {c[1]}" for c in cols if not c[0].startswith('_')]
            schema_parts.append(f"-- {desc}\n{table}:\n" + "\n".join(col_lines))
        schema = "\n\n".join(schema_parts)

        resp = llm.chat.completions.create(
            model='deepseek-chat',
            messages=[
                {'role': 'system', 'content': f"""将自然语言转为 Doris SQL（MySQL 兼容语法）。
规则：只返回 SELECT SQL 不加分号；GMV 在 dws.trade_window_agg 里叫 order_amount；
     排行优先查 ads.category_rank / ads.region_rank，不要自己聚合再排序；
     订单状态、取消率、支付转化率只能查 dwd.order_snapshot（状态问题，不是事件计数）。
方言：用 COUNT(*) 不用 count()；不用 countIf/argMax/toYYYYMM 等 ClickHouse 函数；
     不写 FINAL；日期用 DATE_FORMAT/DATE_SUB；UV 用 BITMAP_UNION_COUNT(uv_bitmap)。
粒度陷阱：dws.trade_window_agg 必须带 window_type 过滤，且 category1_name 与
     region_name 两列必须同时约束（'ALL' 是汇总粒度），只约束一个会重复计数。
     order_user_cnt 只在单窗口内有效，跨窗口/跨天的 UV 必须查 dws.user_active_daily。
表结构：{schema}"""},
                {'role': 'user', 'content': question}
            ],
            temperature=0.1, max_tokens=600
        )
        sql = resp.choices[0].message.content.strip()
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'^```\s*', '', sql)
        sql = re.sub(r'\s*```$', '', sql)
        sql = sql.strip().rstrip(';')

        df = ch.query_df(sql)
        if len(df) == 0:
            return f"SQL：{sql}\n\n查询结果为空"

        result = f"SQL：{sql}\n\n结果：\n{df.head(30).to_markdown(index=False)}"
        if len(df) > 30:
            result += f"\n（共{len(df)}行）"
        return result

    except Exception as e:
        return f"查询失败：{str(e)}"


# ── 工具3：查询知识库 ─────────────────────────────────────────
@tool
def query_knowledge(question: str) -> str:
    """
    查询业务知识库，获取指标定义、字段含义、业务规则等信息。
    适合需要了解业务背景知识时使用。
    输入：关于业务概念的问题，如"GMV怎么定义"。
    输出：知识库中的相关内容。
    """
    try:
        import chromadb
        from chromadb.utils import embedding_functions

        chroma_dir = os.path.join(os.path.dirname(__file__), '..', 'chroma_db')
        client = chromadb.PersistentClient(path=chroma_dir)
        ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name='paraphrase-multilingual-MiniLM-L12-v2'
        )
        col = client.get_collection('ai_dw_knowledge', embedding_function=ef)
        results = col.query(query_texts=[question], n_results=3)

        chunks = []
        for i, doc in enumerate(results['documents'][0]):
            src = results['metadatas'][0][i]['source']
            chunks.append(f"[来源:{src}]\n{doc}")

        return "\n\n---\n\n".join(chunks)

    except Exception as e:
        return f"知识库查询失败：{str(e)}"


# ── 工具4：计算统计指标 ───────────────────────────────────────
@tool
def calculate_stats(data_json: str) -> str:
    """
    对数据进行统计分析，计算均值、标准差、异常值等。
    输入：JSON 格式的数据列表，如 '[{"dt":"2018-01","gmv":100000}, ...]'
    输出：统计分析结果文字描述。
    """
    try:
        data = json.loads(data_json)
        df = pd.DataFrame(data)
        numeric_cols = df.select_dtypes(include='number').columns.tolist()

        results = []
        for col in numeric_cols:
            series = df[col].dropna()
            mean = series.mean()
            std  = series.std()
            # 识别异常值（超过均值±2个标准差）
            lower = mean - 2 * std
            upper = mean + 2 * std
            anomalies = df[series < lower].index.tolist() + df[series > upper].index.tolist()

            results.append(
                f"{col}：均值={mean:.2f}，标准差={std:.2f}，"
                f"正常范围=[{lower:.2f}, {upper:.2f}]"
            )
            if anomalies and len(df.columns) > 1:
                first_col = df.columns[0]
                anomaly_vals = df.iloc[anomalies][first_col].tolist()
                results.append(f"  异常点：{anomaly_vals}")

        return "\n".join(results)

    except Exception as e:
        return f"统计计算失败：{str(e)}"


# ── 工具5：生成文字洞察 ───────────────────────────────────────
@tool
def generate_insight(context: str) -> str:
    """
    根据数据和分析背景，生成专业的业务洞察文字。
    输入：包含数据结果和分析目标的上下文描述。
    输出：3-5句专业的业务洞察。
    """
    try:
        llm = get_llm()
        resp = llm.chat.completions.create(
            model='deepseek-chat',
            messages=[{'role': 'user', 'content':
                f"请根据以下数据分析背景，生成3-5句专业的业务洞察，使用中文，语言简洁有力：\n\n{context}"}],
            temperature=0.7, max_tokens=400
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"洞察生成失败：{str(e)}"


# ── 工具6：保存报告 ───────────────────────────────────────────
@tool
def save_report(content: str) -> str:
    """
    将分析结果保存为 Markdown 报告文件。
    输入：报告内容（Markdown 格式字符串）。
    输出：报告文件的保存路径。
    """
    try:
        reports_dir = os.path.join(os.path.dirname(__file__), '..', 'reports')
        os.makedirs(reports_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"report_{timestamp}.md"
        filepath = os.path.join(reports_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"# AI 数据分析报告\n\n")
            f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("---\n\n")
            f.write(content)

        return f"报告已保存：{filepath}"
    except Exception as e:
        return f"保存失败：{str(e)}"


# 工具列表（供 Agent 使用）
ALL_TOOLS = [
    query_data,
    nl_query,
    query_knowledge,
    calculate_stats,
    generate_insight,
    save_report,
]
