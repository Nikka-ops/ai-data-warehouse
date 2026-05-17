# -*- coding: utf-8 -*-
"""
统一 Agent 工具定义 - 全项目唯一工具源
agents.py 直接从此模块导入，消除重复定义
"""
import os, re, sys
from datetime import datetime
import clickhouse_connect
from langchain.tools import tool

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry, llm_retry

log = get_logger('tools')

# ── 共享 ClickHouse 连接 ──────────────────────────────────────
@ch_retry
def _get_ch():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=120,
    )

def _validate_sql(sql: str) -> str | None:
    sql_upper = sql.strip().upper()
    for kw in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']:
        if re.search(rf'\b{kw}\b', sql_upper):
            return f'错误：不允许执行 {kw} 操作'
    if not (sql_upper.startswith('SELECT') or sql_upper.startswith('WITH')):
        return '错误：只支持 SELECT 查询'
    return None


# ── Tool 1：查询数仓（历史 + 实时表均可）────────────────────
@tool
def query_data(sql: str) -> str:
    """
    在 ClickHouse 数仓执行 SQL 查询，返回结果表格。只支持 SELECT 查询。

    历史批量数据表（2016-2018）：
    - ads.monthly_kpi：ym年月、gmv、order_cnt、user_cnt、avg_order_value、mom_gmv_rate环比
    - dws.order_daily：dt日期、gmv、order_cnt、user_cnt、avg_order_value
    - dws.category_daily：dt、product_category、gmv、order_cnt
    - ads.state_sales_rank：dt_month、state、gmv、order_cnt、rank_by_gmv
    - dwd.order_detail：order_date、state、product_category、price、freight_value、order_status、delivery_days

    实时流数据表（近24小时）：
    - ods.orders_stream：order_id、customer_id、product_category、price、state、order_status、event_time
    - dwd.realtime_order_detail：order_id、product_category、state、price、payment_type、order_status、event_time、event_date
    - dws.realtime_minute_stats：window_start、window_end、order_cnt、total_gmv、avg_price、unique_customers、top_category
    - stream.ai_quality_alerts：alert_time、alert_type、severity、detail、ai_suggestion

    注意：dwd.order_detail 和 ods.orders_stream 没有 gmv 字段，用 price 代替。
    """
    err = _validate_sql(sql)
    if err:
        return err
    try:
        ch = _get_ch()
        df = ch.query_df(sql.strip().rstrip(';'))
        if len(df) == 0:
            return '查询结果为空'
        result = df.head(30).to_markdown(index=False)
        if len(df) > 30:
            result += f'\n\n（共 {len(df)} 行，显示前30行）'
        log.info('[query_data] SQL 执行成功，返回 %d 行', len(df))
        return result
    except Exception as e:
        log.error('[query_data] 执行失败：%s', e)
        return f'查询失败：{e}'


# ── Tool 2：查询知识库 ────────────────────────────────────────
@tool
def query_knowledge(question: str) -> str:
    """
    查询业务知识库，获取指标定义、字段含义、业务规则等。
    适合询问：GMV怎么定义、订单状态含义、某字段是什么意思、业务口径等。
    """
    try:
        import chromadb
        from chromadb.utils import embedding_functions
        client = chromadb.PersistentClient(path=cfg.chroma_dir)
        ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name='paraphrase-multilingual-MiniLM-L12-v2'
        )
        col = client.get_collection('ai_dw_knowledge', embedding_function=ef)
        results = col.query(query_texts=[question], n_results=cfg.rag_top_k)
        chunks = [
            f"[来源:{results['metadatas'][0][i]['source']}]\n{doc[:400]}"
            for i, doc in enumerate(results['documents'][0])
        ]
        return '\n\n---\n\n'.join(chunks)
    except Exception as e:
        log.error('[query_knowledge] 失败：%s', e)
        return f'知识库查询失败：{e}'


# ── Tool 3：异常检测 ──────────────────────────────────────────
@tool
def calculate_anomalies(table: str, date_col: str, value_col: str, where_clause: str = '') -> str:
    """
    对指定表的数值列进行异常检测，找出超过均值±2个标准差的异常点。
    参数：
    - table: 表名，如 dws.order_daily 或 dws.realtime_minute_stats
    - date_col: 日期/时间列名，如 dt 或 window_start
    - value_col: 数值列名，如 gmv 或 total_gmv
    - where_clause: 可选 WHERE 条件，如 "dt >= '2017-01-01'"
    """
    try:
        where = f"WHERE {where_clause}" if where_clause else ""
        sql = f"SELECT {date_col}, {value_col} FROM {table} {where} ORDER BY {date_col}"
        err = _validate_sql(sql)
        if err:
            return err
        ch = _get_ch()
        df = ch.query_df(sql)
        if len(df) == 0:
            return '数据为空'
        mean_v = df[value_col].mean()
        std_v  = df[value_col].std()
        upper, lower = mean_v + 2 * std_v, mean_v - 2 * std_v
        anomalies = df[(df[value_col] > upper) | (df[value_col] < lower)].copy()
        anomalies['偏差倍数'] = ((anomalies[value_col] - mean_v) / std_v).round(2)
        result = (
            f"统计：均值={mean_v:,.0f}，标准差={std_v:,.0f}\n"
            f"正常范围：[{lower:,.0f}, {upper:,.0f}]\n"
            f"发现 {len(anomalies)} 个异常点：\n"
        )
        result += anomalies.head(10).to_markdown(index=False) if len(anomalies) > 0 else "无异常"
        return result
    except Exception as e:
        log.error('[calculate_anomalies] 失败：%s', e)
        return f'异常检测失败：{e}'


# ── Tool 4：生成洞察 ──────────────────────────────────────────
@tool
def generate_insight(context: str) -> str:
    """
    根据数据分析背景生成专业的业务洞察文字（3-6句话）。
    输入：包含数据结果、分析目标的上下文描述。
    输出：中文业务洞察。
    """
    try:
        from openai import OpenAI
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url)
        resp = client.chat.completions.create(
            model=cfg.llm_model,
            messages=[{'role': 'user', 'content':
                f"请根据以下数据分析背景，生成3-6句专业的业务洞察，语言简洁有力，使用中文：\n\n{context}"}],
            temperature=cfg.insight_temperature,
            max_tokens=500,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        log.error('[generate_insight] 失败：%s', e)
        return f'洞察生成失败：{e}'


# ── Tool 5：保存报告 ──────────────────────────────────────────
@tool
def save_report(title: str, content: str) -> str:
    """
    将分析结果保存为 Markdown 报告文件。
    - title: 报告标题
    - content: 报告正文（Markdown 格式）
    返回保存路径。
    """
    try:
        os.makedirs(cfg.reports_dir, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_title = re.sub(r'[\\/:*?"<>|]', '_', title)
        path = os.path.join(cfg.reports_dir, f"{safe_title}_{ts}.md")
        with open(path, 'w', encoding='utf-8') as f:
            f.write(
                f"# {title}\n\n"
                f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n---\n\n{content}"
            )
        log.info('[save_report] 报告已保存：%s', path)
        return f'报告已保存：{path}'
    except Exception as e:
        log.error('[save_report] 保存失败：%s', e)
        return f'保存失败：{e}'


# 导出全部工具
ALL_TOOLS = [query_data, query_knowledge, calculate_anomalies, generate_insight, save_report]
