# -*- coding: utf-8 -*-
"""Agent 工具定义（实时架构，唯一来源）"""
import os, re, sys
from datetime import datetime
import clickhouse_connect
from langchain.tools import tool

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('tools')


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
def query_knowledge(question: str) -> str:
    """
    查询业务知识库，获取指标定义、字段含义、业务规则等概念性信息。
    适合：GMV怎么定义、order_status 各状态含义、payment_type 有哪些类型等。
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
        return '\n\n---\n\n'.join(
            f"[来源:{results['metadatas'][0][i]['source']}]\n{doc[:400]}"
            for i, doc in enumerate(results['documents'][0])
        )
    except Exception as e:
        log.error('[query_knowledge] 失败：%s', e)
        return f'知识库查询失败：{e}'


@tool
def detect_realtime_anomaly(metric: str, lookback_minutes: int = 60) -> str:
    """
    对实时流数据进行异常检测，基于近 lookback_minutes 分钟的历史窗口做 ±2σ 基线对比。
    - metric: 要检测的指标，可选 order_cnt / total_gmv / avg_price
    - lookback_minutes: 基线计算的回溯分钟数（默认60）
    返回：当前窗口与基线的对比结果，以及异常点列表。
    """
    if metric not in ('order_cnt', 'total_gmv', 'avg_price'):
        return '错误：metric 只支持 order_cnt / total_gmv / avg_price'
    try:
        ch = _get_ch()
        df = ch.query_df(f"""
            SELECT window_start, {metric}
            FROM dws.realtime_minute_stats
            WHERE window_start >= now() - INTERVAL {lookback_minutes} MINUTE
            ORDER BY window_start
        """)
        if df.empty:
            return f'最近 {lookback_minutes} 分钟暂无分钟统计数据'

        mean_v = df[metric].mean()
        std_v  = df[metric].std() or 1
        upper, lower = mean_v + 2 * std_v, mean_v - 2 * std_v
        anomalies = df[(df[metric] > upper) | (df[metric] < lower)].copy()
        anomalies['偏差σ'] = ((anomalies[metric] - mean_v) / std_v).round(2)

        result = (
            f"指标：{metric} | 回溯 {lookback_minutes} 分钟 | 共 {len(df)} 个窗口\n"
            f"基线：均值={mean_v:.2f}，标准差={std_v:.2f}\n"
            f"正常范围：[{lower:.2f}, {upper:.2f}]\n"
            f"异常窗口：{len(anomalies)} 个\n"
        )
        if not anomalies.empty:
            result += anomalies.to_markdown(index=False)
        return result
    except Exception as e:
        log.error('[detect_realtime_anomaly] 失败：%s', e)
        return f'异常检测失败：{e}'


@tool
def generate_insight(context: str) -> str:
    """
    根据实时数据分析背景生成专业业务洞察（3-6句话）。
    输入：包含数据结果和分析目标的上下文。输出：中文业务洞察。
    """
    try:
        from openai import OpenAI
        client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url)
        resp = client.chat.completions.create(
            model=cfg.llm_model,
            messages=[{'role': 'user', 'content':
                f"请根据以下实时数据分析背景，生成3-6句专业业务洞察，语言简洁有力，使用中文：\n\n{context}"}],
            temperature=cfg.insight_temperature,
            max_tokens=500,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        log.error('[generate_insight] 失败：%s', e)
        return f'洞察生成失败：{e}'


@tool
def get_etl_status(lookback_hours: int = 1) -> str:
    """
    查询 AI ETL Agent 的最新运行状态和审计日志。
    - lookback_hours: 查询最近 N 小时的审计记录（默认1小时）
    返回：每轮运行的质量分、修复记录数、新生成规则数、状态。
    """
    try:
        ch = _get_ch()
        logs = ch.query_df(f"""
            SELECT run_time, quality_score, records_scanned, issues_found,
                   records_fixed, new_rules_count, status, summary
            FROM stream.etl_audit_log
            WHERE run_time >= now() - INTERVAL {lookback_hours} HOUR
            ORDER BY run_time DESC
            LIMIT 20
        """)
        rules = ch.query_df("""
            SELECT rule_name, rule_type, field_name, hit_count, enabled, ai_reason
            FROM stream.etl_rules
            ORDER BY hit_count DESC, created_at DESC
            LIMIT 20
        """)

        result = f"## AI ETL 最近 {lookback_hours} 小时运行记录\n\n"
        if logs.empty:
            result += "暂无运行记录（Agent 可能尚未启动）\n"
        else:
            result += logs.to_markdown(index=False) + "\n\n"

        result += "## 已生成的清洗规则\n\n"
        if rules.empty:
            result += "暂无规则（数据质量正常或 Agent 尚未运行）\n"
        else:
            result += rules.to_markdown(index=False)

        return result
    except Exception as e:
        log.error('[get_etl_status] 失败：%s', e)
        return f'查询 ETL 状态失败：{e}'


@tool
def get_forecast(metric: str = 'order_cnt', horizon: int = 10) -> str:
    """
    查询实时预测数据（Holt双指数平滑，预测未来N分钟）。
    - metric: 预测指标，可选 order_cnt / total_gmv / avg_price
    - horizon: 查询未来几分钟的预测（1~10，默认10）
    返回：带置信区间的预测值表格。
    """
    if metric not in ('order_cnt', 'total_gmv', 'avg_price'):
        return '错误：metric 只支持 order_cnt / total_gmv / avg_price'
    horizon = max(1, min(10, int(horizon)))
    try:
        ch = _get_ch()
        df = ch.query_df(f"""
            SELECT forecast_time, metric, predicted, lower_bound, upper_bound, horizon
            FROM dws.realtime_forecast
            WHERE metric = '{metric}'
              AND forecast_time >= now()
              AND horizon <= {horizon}
            ORDER BY forecast_time
        """)
        if df.empty:
            return f'暂无 {metric} 的预测数据（预测服务可能尚未启动）'
        return f"## {metric} 未来 {horizon} 分钟预测\n\n" + df.to_markdown(index=False)
    except Exception as e:
        log.error('[get_forecast] 失败：%s', e)
        return f'查询预测失败：{e}'


@tool
def get_proactive_insights(limit: int = 5) -> str:
    """
    获取 AI 主动洞察引擎生成的最新数据洞察报告。
    - limit: 返回最近几条洞察（默认5条）
    返回：洞察标题、类型、内容列表（按时间倒序）。
    """
    limit = max(1, min(20, int(limit)))
    try:
        ch = _get_ch()
        rows = ch.query(f"""
            SELECT generated_at, insight_type, title, content
            FROM stream.proactive_insights
            ORDER BY generated_at DESC
            LIMIT {limit}
        """).result_rows
        if not rows:
            return '暂无主动洞察（洞察引擎可能尚未启动）'
        lines = [f"## AI 主动洞察（最近 {len(rows)} 条）\n"]
        for r in rows:
            lines.append(f"**[{r[1]}] {r[2]}**  \n_{r[0]}_  \n{r[3]}\n")
        return '\n---\n'.join(lines)
    except Exception as e:
        log.error('[get_proactive_insights] 失败：%s', e)
        return f'查询洞察失败：{e}'


@tool
def get_lambda_status(days: int = 7) -> str:
    """
    查询 Lambda 架构批实时对账状态，对比离线层和实时层的数据一致性。
    - days: 查询最近 N 天的对账记录（默认7天）
    返回：每天的批处理量、实时量、差异百分比和一致性状态。
    """
    try:
        ch = _get_ch()
        df = ch.query_df(f"""
            SELECT check_date, batch_order_cnt, stream_order_cnt,
                   cnt_diff_pct, gmv_diff_pct, check_status
            FROM stream.lambda_reconciliation
            WHERE check_time >= now() - INTERVAL {days} DAY
            ORDER BY check_date DESC
            LIMIT 30
        """)
        batch_total = ch.query(
            "SELECT count(), sum(order_cnt) FROM dws.batch_daily_stats"
        ).first_row
        result = f"## Lambda 架构对账状态（最近 {days} 天）\n\n"
        result += f"**离线层总量**：{batch_total[0]} 个分区，{batch_total[1]:,} 条订单\n\n"
        if df.empty:
            result += "暂无对账记录（reconciler 服务可能未启动）\n"
        else:
            result += df.to_markdown(index=False)
        return result
    except Exception as e:
        log.error('[get_lambda_status] 失败：%s', e)
        return f'查询 Lambda 状态失败：{e}'


@tool
def get_alert_investigations(limit: int = 10) -> str:
    """
    查询 AI 告警自动排查记录，了解近期告警的根因分析和处置结果。
    - limit: 返回最近 N 条排查记录（默认10）
    """
    try:
        ch = _get_ch()
        rows = ch.query(f"""
            SELECT investigation_time, alert_type, alert_severity,
                   root_cause, impact_scope, auto_action, status, confidence
            FROM stream.alert_investigations
            ORDER BY investigation_time DESC
            LIMIT {limit}
        """).result_rows
        if not rows:
            return '暂无排查记录（告警排查服务可能未启动）'
        lines = [f"## AI 告警排查记录（最近 {len(rows)} 条）\n"]
        for r in rows:
            lines.append(
                f"**[{r[2]}] {r[1]}** `{str(r[0])[:16]}`  \n"
                f"根因：{r[3]}  \n影响：{r[4]}  \n"
                f"操作：{r[5]}  状态：`{r[7]}`  置信度：{float(r[8] or 0):.0%}\n"
            )
        return '\n---\n'.join(lines)
    except Exception as e:
        log.error('[get_alert_investigations] 失败：%s', e)
        return f'查询排查记录失败：{e}'


ALL_TOOLS = [query_data, query_knowledge, detect_realtime_anomaly,
             generate_insight, get_etl_status, get_forecast,
             get_proactive_insights, get_lambda_status, get_alert_investigations]
