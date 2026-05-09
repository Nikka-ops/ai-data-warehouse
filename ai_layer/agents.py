# -*- coding: utf-8 -*-
"""
三个专用 Agent - 方案A：Tool Calling 模式
使用 LangChain create_tool_calling_agent，DeepSeek 原生 Function Calling
彻底解决 ReAct 格式解析问题
"""

import os
import re
import json
from datetime import datetime
import clickhouse_connect
import pandas as pd
from openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain.tools import tool
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

CH_HOST     = os.getenv('CLICKHOUSE_HOST', 'localhost')
CH_PORT     = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CH_USER     = os.getenv('CLICKHOUSE_USER', 'admin')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'admin123')


def get_ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD
    )

def get_llm(temperature=0.3):
    return ChatOpenAI(
        api_key=os.getenv('DEEPSEEK_API_KEY', ''),
        base_url='https://api.deepseek.com',
        model='deepseek-chat',
        temperature=temperature,
        max_tokens=2000,
        timeout=90,
    )

def get_openai():
    return OpenAI(
        api_key=os.getenv('DEEPSEEK_API_KEY', ''),
        base_url='https://api.deepseek.com',
        timeout=60.0
    )


# ── 工具定义 ──────────────────────────────────────────────────

@tool
def query_data(sql: str) -> str:
    """
    在 ClickHouse 数仓执行 SQL 查询，返回结果表格。
    只支持 SELECT 查询。
    可用表：
    - ads.monthly_kpi：ym年月、gmv、order_cnt、user_cnt、avg_order_value、mom_gmv_rate环比
    - dws.order_daily：dt日期、gmv、order_cnt、user_cnt、avg_order_value
    - dws.category_daily：dt、product_category品类、gmv、order_cnt
    - ads.state_sales_rank：dt_month、state州名、gmv、order_cnt、rank_by_gmv排名
    - dwd.order_detail：order_date、state、product_category、price商品价格、freight_value运费、order_status、delivery_days配送天数
    注意：dwd.order_detail 没有 gmv 字段，用 price 代替。
    """
    try:
        sql_upper = sql.strip().upper()
        for kw in ['INSERT','UPDATE','DELETE','DROP','CREATE','ALTER','TRUNCATE']:
            if re.search(rf'\b{kw}\b', sql_upper):
                return f"错误：不允许执行 {kw} 操作"
        if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
            return "错误：只支持 SELECT 查询"
        ch = get_ch()
        df = ch.query_df(sql.strip().rstrip(';'))
        if len(df) == 0:
            return "查询结果为空"
        result = df.head(30).to_markdown(index=False)
        if len(df) > 30:
            result += f"\n\n（共 {len(df)} 行，显示前30行）"
        return result
    except Exception as e:
        return f"查询失败：{str(e)}"


@tool
def query_knowledge(question: str) -> str:
    """
    查询业务知识库，获取指标定义、字段含义、业务规则等。
    适合询问：GMV怎么定义、订单状态含义、某字段是什么意思、业务口径等问题。
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
            chunks.append(f"[来源:{src}]\n{doc[:400]}")
        return "\n\n---\n\n".join(chunks)
    except Exception as e:
        return f"知识库查询失败：{str(e)}"


@tool
def calculate_anomalies(table: str, date_col: str, value_col: str, where_clause: str = "") -> str:
    """
    对指定表的数值列进行异常检测，找出超过均值±2个标准差的异常点。
    参数：
    - table: 表名，如 dws.order_daily
    - date_col: 日期列名，如 dt
    - value_col: 数值列名，如 gmv
    - where_clause: 可选的 WHERE 条件，如 "dt >= '2017-01-01'"
    """
    try:
        where = f"WHERE {where_clause}" if where_clause else ""
        sql = f"SELECT {date_col}, {value_col} FROM {table} {where} ORDER BY {date_col}"
        ch = get_ch()
        df = ch.query_df(sql)
        if len(df) == 0:
            return "数据为空"
        mean_v = df[value_col].mean()
        std_v  = df[value_col].std()
        upper  = mean_v + 2 * std_v
        lower  = mean_v - 2 * std_v
        anomalies = df[(df[value_col] > upper) | (df[value_col] < lower)].copy()
        anomalies['偏差倍数'] = ((anomalies[value_col] - mean_v) / std_v).round(2)
        result = (
            f"统计结果：均值={mean_v:,.0f}，标准差={std_v:,.0f}\n"
            f"正常范围：[{lower:,.0f}, {upper:,.0f}]\n"
            f"发现 {len(anomalies)} 个异常点：\n"
        )
        if len(anomalies) > 0:
            result += anomalies.head(10).to_markdown(index=False)
        else:
            result += "无异常"
        return result
    except Exception as e:
        return f"异常检测失败：{str(e)}"


@tool
def generate_insight(context: str) -> str:
    """
    根据数据分析背景生成专业的业务洞察文字（3-6句话）。
    输入：包含数据结果、分析目标的上下文描述。
    输出：中文业务洞察。
    """
    try:
        llm = get_openai()
        resp = llm.chat.completions.create(
            model='deepseek-chat',
            messages=[{'role': 'user', 'content':
                f"请根据以下数据分析背景，生成3-6句专业的业务洞察，语言简洁有力，使用中文：\n\n{context}"}],
            temperature=0.6,
            max_tokens=500,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"洞察生成失败：{str(e)}"


@tool
def save_report(title: str, content: str) -> str:
    """
    将分析结果保存为 Markdown 报告文件。
    参数：
    - title: 报告标题
    - content: 报告正文（Markdown 格式）
    返回：保存路径。
    """
    try:
        reports_dir = os.path.join(os.path.dirname(__file__), '..', 'reports')
        os.makedirs(reports_dir, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_title = re.sub(r'[\\/:*?"<>|]', '_', title)
        path = os.path.join(reports_dir, f"{safe_title}_{ts}.md")
        with open(path, 'w', encoding='utf-8') as f:
            f.write(f"# {title}\n\n生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n\n{content}")
        return f"报告已保存：{path}"
    except Exception as e:
        return f"保存失败：{str(e)}"


# ── Agent 工厂 ────────────────────────────────────────────────

def make_prompt(system_msg: str) -> ChatPromptTemplate:
    return ChatPromptTemplate.from_messages([
        ("system", system_msg),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ])

def make_executor(tools: list, system_msg: str, max_iter: int = 10) -> AgentExecutor:
    llm = get_llm()
    prompt = make_prompt(system_msg)
    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        max_iterations=max_iter,
        return_intermediate_steps=True,
    )


# ══════════════════════════════════════════════════════════════
# Agent 1：销售异常分析
# ══════════════════════════════════════════════════════════════

ANOMALY_SYSTEM = """你是一位专业的数据分析师，擅长销售异常检测。

请按以下步骤完成分析：
1. 用 calculate_anomalies 检测 dws.order_daily 表 gmv 字段的异常，where条件用 dt >= '2017-01-01'
2. 用 query_data 查询异常峰值日期（2017-11-24）前后14天的每日数据
3. 用 query_knowledge 查询"巴西电商节日规律"
4. 用 generate_insight 生成异常原因分析，输入前3步的完整结果
5. 用 save_report 保存完整报告，标题"销售异常分析报告"，内容包含异常数据、对比数据、节日背景和洞察

完成所有步骤后给出最终结论。"""

def run_anomaly_agent(callback=None):
    tools = [query_data, calculate_anomalies, query_knowledge, generate_insight, save_report]
    executor = make_executor(tools, ANOMALY_SYSTEM, max_iter=8)
    result = executor.invoke({"input": "请对销售数据进行异常检测分析，找出异常日期并分析原因"})
    return result


# ══════════════════════════════════════════════════════════════
# Agent 2：自动周报
# ══════════════════════════════════════════════════════════════

WEEKLY_SYSTEM = """你是一位数据分析师，负责生成每周数据报告。

请按以下步骤生成完整周报：
1. 用 query_data 查询月度KPI：
   SELECT ym, round(gmv,0) AS GMV, order_cnt AS 订单数, user_cnt AS 用户数, round(mom_gmv_rate,2) AS 环比增长率 FROM ads.monthly_kpi ORDER BY ym DESC LIMIT 6
2. 用 query_data 查询品类Top10：
   SELECT product_category AS 品类, round(sum(gmv),0) AS 总GMV, sum(order_cnt) AS 订单数 FROM dws.category_daily GROUP BY product_category ORDER BY 总GMV DESC LIMIT 10
3. 用 query_data 查询最新月省份排行：
   SELECT state AS 州, round(gmv,0) AS GMV, order_cnt AS 订单数, rank_by_gmv AS 排名 FROM ads.state_sales_rank WHERE dt_month=(SELECT max(dt_month) FROM ads.state_sales_rank) ORDER BY rank_by_gmv LIMIT 10
4. 用 generate_insight 分别对GMV趋势、品类数据生成洞察
5. 用 save_report 将以上全部内容整合为Markdown周报保存，标题"数据分析周报"

最终输出周报摘要。"""

def run_weekly_report_agent(callback=None):
    tools = [query_data, generate_insight, save_report]
    executor = make_executor(tools, WEEKLY_SYSTEM, max_iter=8)
    result = executor.invoke({"input": "请生成本周数据分析周报"})
    return result


# ══════════════════════════════════════════════════════════════
# Agent 3：自由分析
# ══════════════════════════════════════════════════════════════

FREE_SYSTEM = """你是一位专业的数据分析师，拥有以下工具：
- query_data：执行SQL查询数仓数据
- query_knowledge：查询业务知识库获取指标定义和业务规则
- generate_insight：根据数据生成业务洞察
- save_report：保存分析报告

可用数据表：
- ads.monthly_kpi：ym、gmv、order_cnt、user_cnt、mom_gmv_rate
- dws.category_daily：dt、product_category、gmv、order_cnt
- dws.order_daily：dt、gmv、order_cnt、user_cnt、avg_order_value
- ads.state_sales_rank：dt_month、state、gmv、order_cnt、rank_by_gmv
- dwd.order_detail：order_date、state、product_category、price、order_status、delivery_days

工作原则：
- 先理解分析目标，再决定查哪些数据
- 查完数据后用 generate_insight 生成洞察
- 最后用 save_report 保存完整报告
- 用中文回答，结论要具体有数字支撑"""

def run_free_agent(user_goal: str, callback=None):
    tools = [query_data, query_knowledge, generate_insight, save_report]
    executor = make_executor(tools, FREE_SYSTEM, max_iter=10)
    result = executor.invoke({"input": user_goal})
    return result


# ── 命令行测试 ────────────────────────────────────────────────
if __name__ == '__main__':
    import sys

    print("=" * 60)
    print("  Agent 测试（Tool Calling 模式）")
    print("=" * 60)

    mode = sys.argv[1] if len(sys.argv) > 1 else '3'

    if mode == '1':
        print("\n[Agent 1] 销售异常分析")
        result = run_anomaly_agent()
    elif mode == '2':
        print("\n[Agent 2] 自动周报生成")
        result = run_weekly_report_agent()
    else:
        print("\n[Agent 3] 自由分析")
        result = run_free_agent(
            "分析2018年上半年销售趋势，找出GMV最高的3个月和最低的3个月，给出原因分析"
        )

    print("\n" + "=" * 60)
    print("最终结论：")
    print(result['output'])
    print(f"\n执行步骤数：{len(result.get('intermediate_steps', []))}")