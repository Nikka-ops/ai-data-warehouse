# -*- coding: utf-8 -*-
"""
慢查询诊断模块
定期扫描 ClickHouse system.query_log，找出慢查询，
调用 LLM 给出优化建议，存入 stream.slow_query_analysis。
"""

import os
import sys
import json
import time
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import clickhouse_connect
from openai import OpenAI

from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry, llm_retry

log = get_logger('slow_query_analyzer')

# ── 常量 ──────────────────────────────────────────────────────
SLOW_THRESHOLD_MS = 3000          # 慢查询阈值（毫秒）
SCAN_WINDOW_HOURS = 1             # 扫描最近1小时的查询
DEDUP_WINDOW_HOURS = 24           # 相同 SQL 24小时内只分析一次
DEFAULT_INTERVAL = 1800           # 默认扫描间隔（秒）

TARGET_DB = 'stream'
TARGET_TABLE = 'slow_query_analysis'
TARGET_FULL = f'{TARGET_DB}.{TARGET_TABLE}'

VALID_CATEGORIES = {'MISSING_INDEX', 'FULL_SCAN', 'INEFFICIENT_JOIN', 'OTHER'}

# ── LLM 客户端 ────────────────────────────────────────────────
llm = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url)

# ── Prompt ────────────────────────────────────────────────────
ANALYZE_PROMPT = (
    '你是 ClickHouse 专家。以下 SQL 执行了 {duration_ms}ms，扫描了 {read_rows} 行，'
    '{read_bytes_mb:.1f}MB。\n'
    'SQL：{sql}\n'
    '请给出：1) category（MISSING_INDEX/FULL_SCAN/INEFFICIENT_JOIN/OTHER）'
    ' 2) 优化建议（120字内）\n'
    '以 JSON 格式输出：{{"category": "...", "suggestion": "..."}}'
)

# ── DDL ───────────────────────────────────────────────────────
CREATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_FULL} (
    analyzed_at    DateTime DEFAULT now(),
    query_time     DateTime,
    duration_ms    UInt64,
    query_sql      String,
    read_rows      UInt64,
    read_bytes     UInt64,
    suggestion     String,
    category       String
) ENGINE = ReplacingMergeTree(analyzed_at)
ORDER BY (query_time, query_sql)
TTL analyzed_at + INTERVAL 30 DAY
"""


# ── ClickHouse 工具函数 ────────────────────────────────────────

@ch_retry
def _get_ch():
    return clickhouse_connect.get_client(
        host=cfg.ch_host,
        port=cfg.ch_port,
        username=cfg.ch_user,
        password=cfg.ch_password,
        connect_timeout=10,
        send_receive_timeout=60,
    )


@ch_retry
def _ensure_table(ch):
    """确保目标表存在"""
    ch.command(CREATE_TABLE_DDL)
    log.info('确认目标表 %s 存在', TARGET_FULL)


# ── LLM 分析 ─────────────────────────────────────────────────

@llm_retry
def _analyze_with_llm(sql: str, duration_ms: int, read_rows: int, read_bytes: int) -> dict:
    """调用 LLM 分析慢查询，返回 {'category': ..., 'suggestion': ...}"""
    read_bytes_mb = read_bytes / (1024 * 1024)
    prompt = ANALYZE_PROMPT.format(
        duration_ms=duration_ms,
        read_rows=read_rows,
        read_bytes_mb=read_bytes_mb,
        sql=sql[:2000],   # 截断超长 SQL，避免超 token
    )

    resp = llm.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': prompt}],
        temperature=0.1,
        max_tokens=300,
    )
    raw = resp.choices[0].message.content.strip()

    # 从响应中提取 JSON（兼容 LLM 在 JSON 外包裹 markdown 的情况）
    json_match = re.search(r'\{.*?\}', raw, re.DOTALL)
    if json_match:
        result = json.loads(json_match.group())
    else:
        result = json.loads(raw)

    category = result.get('category', 'OTHER').strip().upper()
    if category not in VALID_CATEGORIES:
        category = 'OTHER'

    suggestion = result.get('suggestion', '').strip()[:120]

    return {'category': category, 'suggestion': suggestion}


# ── 核心扫描逻辑 ──────────────────────────────────────────────

@ch_retry
def _fetch_slow_queries(ch) -> list[dict]:
    """从 system.query_log 拉取最近1小时的慢查询"""
    query = f"""
        SELECT
            query_start_time,
            query_duration_ms,
            query,
            read_rows,
            read_bytes,
            result_rows,
            memory_usage,
            user
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query_duration_ms > {SLOW_THRESHOLD_MS}
          AND query_start_time >= now() - INTERVAL {SCAN_WINDOW_HOURS} HOUR
          AND NOT startsWith(lower(trimLeft(query)), 'system')
        ORDER BY query_duration_ms DESC
        LIMIT 50
    """
    rows = ch.query(query).result_rows
    result = []
    for r in rows:
        result.append({
            'query_time':   r[0],
            'duration_ms':  int(r[1]),
            'query_sql':    r[2],
            'read_rows':    int(r[3]),
            'read_bytes':   int(r[4]),
            'result_rows':  int(r[5]),
            'memory_usage': int(r[6]),
            'user':         r[7],
        })
    log.info('从 system.query_log 获取到 %d 条慢查询', len(result))
    return result


@ch_retry
def _fetch_analyzed_sqls(ch) -> set[str]:
    """查询最近24小时内已分析过的 query_sql 集合，用于去重"""
    rows = ch.query(
        f"SELECT DISTINCT query_sql FROM {TARGET_FULL} "
        f"WHERE analyzed_at >= now() - INTERVAL {DEDUP_WINDOW_HOURS} HOUR"
    ).result_rows
    return {r[0] for r in rows}


@ch_retry
def _insert_analysis(ch, records: list[dict]):
    """批量写入分析结果"""
    if not records:
        return

    column_names = [
        'query_time', 'duration_ms', 'query_sql',
        'read_rows', 'read_bytes', 'suggestion', 'category',
    ]
    data = [
        [
            rec['query_time'],
            rec['duration_ms'],
            rec['query_sql'],
            rec['read_rows'],
            rec['read_bytes'],
            rec['suggestion'],
            rec['category'],
        ]
        for rec in records
    ]
    ch.insert(TARGET_TABLE, data, column_names=column_names, database=TARGET_DB)
    log.info('成功写入 %d 条慢查询分析结果到 %s', len(data), TARGET_FULL)


def run_once():
    """执行一次慢查询扫描与分析"""
    log.info('开始慢查询扫描...')
    try:
        ch = _get_ch()
        _ensure_table(ch)

        slow_queries = _fetch_slow_queries(ch)
        if not slow_queries:
            log.info('本次扫描未发现慢查询，退出')
            return

        already_analyzed = _fetch_analyzed_sqls(ch)
        log.info('24小时内已分析 %d 条 SQL，将进行去重', len(already_analyzed))

        to_analyze = [q for q in slow_queries if q['query_sql'] not in already_analyzed]
        log.info('去重后剩余 %d 条待分析慢查询', len(to_analyze))

        results = []
        for i, q in enumerate(to_analyze):
            log.info(
                '[%d/%d] 分析慢查询 duration=%dms rows=%d: %s...',
                i + 1, len(to_analyze),
                q['duration_ms'], q['read_rows'],
                q['query_sql'][:80].replace('\n', ' '),
            )
            try:
                analysis = _analyze_with_llm(
                    sql=q['query_sql'],
                    duration_ms=q['duration_ms'],
                    read_rows=q['read_rows'],
                    read_bytes=q['read_bytes'],
                )
                results.append({
                    **q,
                    'category':   analysis['category'],
                    'suggestion': analysis['suggestion'],
                })
                log.info('  → category=%s suggestion=%s', analysis['category'], analysis['suggestion'][:60])
            except Exception as e:
                log.warning('LLM 分析失败（跳过）: %s', e)
                results.append({
                    **q,
                    'category':   'OTHER',
                    'suggestion': f'LLM 分析失败: {str(e)[:80]}',
                })

        _insert_analysis(ch, results)
        log.info('本次扫描完成，共分析并写入 %d 条慢查询', len(results))

    except Exception as e:
        log.error('慢查询扫描异常: %s', e, exc_info=True)


def run_loop(interval: int = DEFAULT_INTERVAL):
    """每30分钟扫描一次（阻塞循环）"""
    log.info('慢查询诊断服务启动，扫描间隔 %d 秒', interval)
    while True:
        run_once()
        log.info('下次扫描将在 %d 秒后执行', interval)
        time.sleep(interval)


@ch_retry
def get_recent_analysis(ch=None, limit: int = 20) -> list[dict]:
    """供 dashboard 调用，返回最近的慢查询分析结果"""
    if ch is None:
        ch = _get_ch()

    rows = ch.query(
        f"""
        SELECT
            analyzed_at,
            query_time,
            duration_ms,
            query_sql,
            read_rows,
            read_bytes,
            suggestion,
            category
        FROM {TARGET_FULL}
        ORDER BY analyzed_at DESC
        LIMIT {limit}
        """
    ).result_rows

    return [
        {
            'analyzed_at': r[0],
            'query_time':  r[1],
            'duration_ms': r[2],
            'query_sql':   r[3],
            'read_rows':   r[4],
            'read_bytes':  r[5],
            'suggestion':  r[6],
            'category':    r[7],
        }
        for r in rows
    ]


if __name__ == '__main__':
    run_loop()
