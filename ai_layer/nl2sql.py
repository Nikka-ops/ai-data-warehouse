# -*- coding: utf-8 -*-
"""NL2SQL：自然语言 → 实时 ClickHouse SQL（Self-RAG 自我验证版）"""
import os
import re
import sys
import time

import clickhouse_connect
import pandas as pd
from openai import OpenAI

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import llm_retry, ch_retry

log = get_logger('nl2sql')

llm = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url)


@ch_retry
def get_ch_client():
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


# ── 实时表描述（全部为流式数据）────────────────────────────────
TABLE_DESCRIPTIONS = {
    'ods.orders_stream': (
        '实时订单原始流，由 Kafka 消费写入。'
        '字段：order_id/customer_id/product_id/product_category/seller_id/'
        'price(商品价格)/freight_value(运费)/order_status/state(州)/city/event_time/_ingest_time'
    ),
    'ods.payments_stream': (
        '实时支付原始流，由 Kafka 消费写入。'
        '字段：payment_id/order_id/payment_type/payment_value/installments/event_time'
    ),
    'dwd.realtime_order_detail': (
        '实时订单+支付关联宽表，由 Flink JOIN 生成。'
        '字段：order_id/customer_id/product_category/state/city/'
        'price/freight_value/total_amount/payment_type/payment_value/'
        'order_status/event_time/event_date/event_hour/is_paid'
    ),
    'dws.realtime_minute_stats': (
        '实时分钟级聚合统计，由 Flink 1分钟滚动窗口计算。'
        '字段：window_start/window_end/order_cnt/total_gmv/avg_price/unique_customers/top_category'
    ),
    'stream.ai_quality_alerts': (
        '实时 AI 异常告警，含规则检测和 AI 原因分析。'
        '字段：alert_time/alert_type(ANOMALY|QUALITY)/severity(HIGH|MEDIUM|LOW)/'
        'field_name/detail/ai_suggestion/window_start/window_end/metric_value/threshold_value'
    ),
    'ads.realtime_hourly': (
        '今日小时销售汇总视图（自动过滤 event_time >= today()）。'
        '字段：hour_start/order_cnt/gmv/avg_price/unique_customers/cancel_cnt'
    ),
    'ads.realtime_category_today': (
        '今日品类销售排行视图（按 gmv 降序）。'
        '字段：product_category/order_cnt/gmv/avg_price'
    ),
    'ads.realtime_state_today': (
        '今日各州销售排行视图。'
        '字段：state/order_cnt/gmv/rank_by_gmv'
    ),
}

# ── Schema 缓存（TTL 自动失效）────────────────────────────────
_schema_cache: dict = {}


@ch_retry
def get_schema(client=None) -> str:
    if _schema_cache.get('schema') and (time.time() - _schema_cache.get('_ts', 0)) < cfg.schema_cache_ttl:
        return _schema_cache['schema']

    if client is None:
        client = get_ch_client()

    log.info('刷新实时 Schema 缓存...')
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
            log.warning('无法获取 %s 的 Schema：%s', table, e)

    schema = "\n\n".join(parts)
    _schema_cache.update({'schema': schema, '_ts': time.time()})
    log.info('Schema 缓存更新完成，包含 %d 张表', len(parts))
    return schema


def invalidate_schema_cache():
    _schema_cache.clear()


# ── Prompt ────────────────────────────────────────────────────
SYSTEM_PROMPT = """你是一位精通 ClickHouse SQL 的实时数据分析师。
根据下方表结构将用户的自然语言问题转换为可执行的 ClickHouse SQL。

【数据背景】
本系统为纯实时流处理架构，所有数据来自 Kafka 实时流，由 Flink 处理后写入 ClickHouse。
无历史批量数据，数据时效取决于 Kafka 消息保留策略（默认24小时）和 ClickHouse TTL 设置。

【查询路由规则】
- 查原始订单/支付明细               → ods.orders_stream / ods.payments_stream
- 查订单+支付关联数据               → dwd.realtime_order_detail
- 查分钟级流量趋势                  → dws.realtime_minute_stats
- 查今日小时趋势                    → ads.realtime_hourly（已过滤 today()，直接 SELECT）
- 查今日品类排行                    → ads.realtime_category_today（已排序，直接 SELECT）
- 查今日各州排行                    → ads.realtime_state_today（直接 SELECT）
- 查异常告警                        → stream.ai_quality_alerts
- 查"最近N分钟"数据                 → WHERE event_time >= now() - INTERVAL N MINUTE
- 查"今天"数据                      → WHERE event_time >= today()

【SQL 规则】
- 金额字段用 price（商品价）或 total_amount，没有 gmv 字段（用 sum(price) 代替）
- 排行用 ORDER BY xxx DESC LIMIT N
- 时间过滤优先用 event_time 字段
- 数字保留2位小数用 round()
- ads.* 视图已内置 today() 过滤，不要再加时间条件

【表结构】
{schema}

{history_section}
【输出要求】
1. 只返回 SQL，不加任何解释
2. 禁止 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE
3. SQL 末尾不加分号
4. 如果用户的问题是对上一轮的追问（如"再加一个字段"、"改成按州分组"），请在上一轮 SQL 基础上修改
"""

REPAIR_PROMPT = """你是一位精通 ClickHouse SQL 的数据库专家。
以下 SQL 在执行时产生了错误，请修复它。

【用户原始问题】
{question}

【错误的 SQL】
{bad_sql}

【ClickHouse 报错信息】
{error}

【可用表结构】
{schema}

【修复要求】
1. 只返回修复后的 SQL，不加任何解释
2. 禁止 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE
3. SQL 末尾不加分号
4. 针对报错信息进行精准修复
"""

INSIGHT_PROMPT = """你是实时数据分析师，根据以下实时查询结果给出3-5句业务洞察。
问题：{question}
SQL：{sql}
结果（前10行）：
{data}
要求：直接给出洞察，引用具体数字，说明实时业务状态，用中文。
"""

CONSERVATIVE_INSIGHT_PROMPT = """你是实时数据分析师，请严格根据以下查询结果给出3-5句保守的业务洞察。
问题：{question}
SQL：{sql}
结果（前10行）：
{data}
【严格要求】
- 只陈述数据中直接呈现的事实，不推测、不延伸
- 每句话必须有数据支撑，不得引入结果之外的信息
- 如数据量不足，直接说明，不要编造趋势判断
- 用中文回答
"""

SCORE_INSIGHT_PROMPT = """你是数据分析质检专家，判断以下洞察是否完全基于查询结果，没有编造或推测结果之外的内容。

【查询结果摘要】
{data_summary}

【待评估的洞察】
{insight}

评判标准：
- 1.0：洞察完全来自数据，每个结论都有数据支撑
- 0.7~0.9：大部分基于数据，有少量合理推断
- 0.4~0.6：部分基于数据，存在明显推断或延伸
- 0.0~0.3：大量编造，与数据严重不符

只输出一个 0 到 1 之间的小数，不要输出其他任何内容。"""


def _format_nl2sql_history(history: list[dict]) -> str:
    """将最近 N 轮对话格式化为 Prompt 片段"""
    if not history:
        return ''
    lines = ['【对话历史（请结合上下文理解追问意图）】']
    for turn in history[-3:]:  # 最多保留最近3轮
        lines.append(f"用户问题：{turn['question']}")
        lines.append(f"生成SQL：{turn['sql']}")
        if turn.get('result_summary'):
            lines.append(f"结果摘要：{turn['result_summary']}")
        lines.append('')
    return '\n'.join(lines) + '\n'


def _clean_sql(raw: str) -> str:
    """清除 LLM 返回的 SQL 中的代码块标记"""
    sql = raw.strip()
    sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r'^```\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip().rstrip(';')


@llm_retry
def generate_sql(question: str, schema: str, history: list[dict] | None = None) -> str:
    history_section = _format_nl2sql_history(history or [])
    resp = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT.format(
                schema=schema, history_section=history_section
            )},
            {'role': 'user', 'content': question},
        ],
        temperature=cfg.nl2sql_temperature,
        max_tokens=800,
    )
    return _clean_sql(resp.choices[0].message.content)


@llm_retry
def generate_insight(question: str, sql: str, df: pd.DataFrame,
                     conservative: bool = False) -> str:
    prompt_tpl = CONSERVATIVE_INSIGHT_PROMPT if conservative else INSIGHT_PROMPT
    resp = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': prompt_tpl.format(
            question=question, sql=sql, data=df.head(10).to_markdown(index=False)
        )}],
        temperature=cfg.insight_temperature,
        max_tokens=400,
    )
    return resp.choices[0].message.content.strip()


def validate_sql(sql: str):
    upper = sql.strip().upper()
    for kw in ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']:
        if re.search(rf'\b{kw}\b', upper):
            raise ValueError(f'不允许执行 {kw} 操作')
    if not upper.startswith('SELECT') and not upper.startswith('WITH'):
        raise ValueError('SQL 必须以 SELECT 或 WITH 开头')


def _make_result_summary(df: pd.DataFrame, max_len: int = 120) -> str:
    """生成结果摘要供下一轮对话使用"""
    if df.empty:
        return '结果为空'
    num_cols = df.select_dtypes(include='number').columns.tolist()
    parts = [f'{len(df)} 行']
    for col in num_cols[:2]:
        parts.append(f'{col} 范围 {df[col].min():.1f}~{df[col].max():.1f}')
    return '，'.join(parts)[:max_len]


# ── Self-RAG 核心辅助函数 ──────────────────────────────────────

def _explain_sql(ch, sql: str) -> tuple[bool, str]:
    """用 EXPLAIN SYNTAX 验证 SQL，返回 (is_valid, error_msg)。"""
    try:
        ch.command(f"EXPLAIN SYNTAX {sql}")
        return True, ""
    except Exception as e:
        return False, str(e)


def _repair_sql(original_q: str, bad_sql: str, error: str, schema: str) -> str:
    """让 LLM 修复错误的 SQL，返回修复后的 SQL。"""
    try:
        resp = llm.chat.completions.create(
            model=cfg.llm_model,
            messages=[
                {'role': 'user', 'content': REPAIR_PROMPT.format(
                    question=original_q,
                    bad_sql=bad_sql,
                    error=error,
                    schema=schema,
                )},
            ],
            temperature=cfg.nl2sql_temperature,
            max_tokens=800,
        )
        return _clean_sql(resp.choices[0].message.content)
    except Exception as e:
        log.error('[SQL修复] LLM 调用失败：%s', e)
        return bad_sql  # 降级：返回原始错误 SQL


def _score_insight(insight: str, data_summary: str) -> float:
    """自评置信分：0-1，判断洞察是否基于查询结果。LLM 失败时降级返回 0.5。"""
    try:
        resp = llm.chat.completions.create(
            model=cfg.llm_model,
            messages=[
                {'role': 'user', 'content': SCORE_INSIGHT_PROMPT.format(
                    data_summary=data_summary,
                    insight=insight,
                )},
            ],
            temperature=0,
            max_tokens=10,
        )
        raw = resp.choices[0].message.content.strip()
        score = float(re.search(r'[0-9]*\.?[0-9]+', raw).group())
        score = max(0.0, min(1.0, score))
        log.info('[洞察评分] %.2f', score)
        return score
    except Exception as e:
        log.warning('[洞察评分] 评分失败，降级为 0.5：%s', e)
        return 0.5


# ── 对外接口 ──────────────────────────────────────────────────

def get_schema_context() -> str:
    """返回当前 Schema 上下文字符串（兼容外部模块调用）。"""
    return get_schema()


def nl2sql(question: str, with_insight: bool = True,
           history: list[dict] | None = None,
           session_id: str | None = None) -> dict:
    """
    Self-RAG NL2SQL 主入口。

    history 格式（每轮一个 dict）：
      [{'question': str, 'sql': str, 'result_summary': str}, ...]

    返回 dict 新增字段：
      repair_attempts  : int   — SQL 修复尝试次数（0 = 首次生成即通过）
      insight_confidence: float — 洞察自评置信分（0-1）
    """
    result = {
        'question': question,
        'sql': '',
        'data': pd.DataFrame(),
        'insight': '',
        'row_count': 0,
        'error': None,
        'result_summary': '',
        'repair_attempts': 0,
        'insight_confidence': 0.0,
    }
    try:
        client = get_ch_client()
        schema = get_schema(client)

        log.info('[NL2SQL] session=%s question=%s', session_id, question)

        # ── 步骤1：生成 SQL ───────────────────────────────────────
        sql = generate_sql(question, schema, history=history)
        result['sql'] = sql
        log.info('[生成SQL] %s', sql)

        validate_sql(sql)

        # ── 步骤2：EXPLAIN 验证 + Self-RAG 修复（最多2次）────────
        MAX_REPAIRS = 2
        is_valid, err_msg = _explain_sql(client, sql)

        repair_attempts = 0
        while not is_valid and repair_attempts < MAX_REPAIRS:
            repair_attempts += 1
            log.warning('[EXPLAIN失败] 第%d次修复，错误：%s', repair_attempts, err_msg)
            sql = _repair_sql(question, sql, err_msg, schema)
            result['sql'] = sql
            log.info('[修复SQL] 第%d次：%s', repair_attempts, sql)

            # 修复后再次检查安全规则
            try:
                validate_sql(sql)
            except ValueError as ve:
                result['error'] = str(ve)
                result['repair_attempts'] = repair_attempts
                log.error('[安全校验] 修复后 SQL 仍含危险操作：%s', ve)
                return result

            is_valid, err_msg = _explain_sql(client, sql)

        result['repair_attempts'] = repair_attempts

        if not is_valid:
            # 2 次修复均失败：返回错误 + 最后一次 SQL 供用户参考
            result['error'] = (
                f'SQL 验证失败（已尝试修复 {MAX_REPAIRS} 次）。'
                f'最后错误：{err_msg}。'
                f'参考 SQL（未执行）：{sql}'
            )
            log.error('[Self-RAG] %d 次修复均失败，放弃执行', MAX_REPAIRS)
            return result

        # ── 步骤3：执行 SQL ──────────────────────────────────────
        df = client.query_df(sql)
        result['data'] = df
        result['row_count'] = len(df)
        result['result_summary'] = _make_result_summary(df)
        log.info('[查询完成] %d 行', len(df))

        # ── 步骤4：生成洞察 + Self-RAG 置信评分 ─────────────────
        if with_insight and len(df) > 0:
            data_summary = df.head(10).to_markdown(index=False)

            insight = generate_insight(question, sql, df, conservative=False)
            confidence = _score_insight(insight, data_summary)
            result['insight_confidence'] = confidence

            if confidence < 0.7:
                log.info('[Self-RAG] 置信分 %.2f < 0.7，用保守 prompt 重新生成洞察', confidence)
                insight = generate_insight(question, sql, df, conservative=True)
                # 重新评分（仅做记录，不再二次迭代）
                confidence2 = _score_insight(insight, data_summary)
                result['insight_confidence'] = confidence2
                log.info('[Self-RAG] 保守洞察置信分 %.2f', confidence2)

            result['insight'] = insight
            log.info(
                '[NL2SQL完成] repair_attempts=%d insight_confidence=%.2f',
                repair_attempts, result['insight_confidence'],
            )

    except Exception as e:
        result['error'] = str(e)
        log.error('[NL2SQL错误] %s', e)

    return result
