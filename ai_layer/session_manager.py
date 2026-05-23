# -*- coding: utf-8 -*-
"""
会话管理器：将多轮对话历史持久化到 ClickHouse stream.chat_sessions。
支持跨浏览器会话的上下文恢复。
"""
import os
import sys
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('session_mgr')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=5, send_receive_timeout=20,
    )


def new_session_id() -> str:
    return str(uuid.uuid4())


def save_turn(session_id: str, turn_index: int, role: str,
              msg_type: str = '', content: str = '',
              sql_text: str = '', result_summary: str = '', sources: str = ''):
    """将单轮对话写入 ClickHouse（best-effort，失败不抛异常）"""
    try:
        _get_ch().insert(
            'stream.chat_sessions',
            [[session_id, '', turn_index, role, msg_type,
              content[:4000], sql_text[:2000], result_summary[:500],
              sources[:500], datetime.now()]],
            column_names=['session_id', 'session_name', 'turn_index', 'role',
                          'msg_type', 'content', 'sql_text', 'result_summary',
                          'sources', 'created_at'],
        )
    except Exception as e:
        log.warning('保存对话轮次失败（非关键）：%s', e)


def rename_session(session_id: str, name: str):
    """为会话设置名称（INSERT 覆盖第0行的 session_name）"""
    try:
        ch = _get_ch()
        ch.command(f"""
            ALTER TABLE stream.chat_sessions
            UPDATE session_name = '{name.replace("'", "''")}'
            WHERE session_id = '{session_id}'
        """)
    except Exception as e:
        log.warning('重命名会话失败：%s', e)


def list_recent_sessions(limit: int = 10) -> list[dict]:
    """
    列出最近的会话，每条包含：
      session_id, session_name, first_question, turn_count, started_at
    """
    try:
        rows = _get_ch().query(f"""
            SELECT
                session_id,
                any(session_name)                      AS session_name,
                argMin(content, turn_index)            AS first_question,
                countIf(role = 'user')                 AS turn_count,
                min(created_at)                        AS started_at
            FROM stream.chat_sessions
            WHERE role = 'user' AND length(content) > 0
            GROUP BY session_id
            ORDER BY started_at DESC
            LIMIT {limit}
        """).result_rows
        return [
            {
                'session_id':    r[0],
                'session_name':  r[1] or '',
                'first_question': r[2][:50],
                'turn_count':    r[3],
                'started_at':    r[4],
            }
            for r in rows
        ]
    except Exception as e:
        log.warning('获取会话列表失败：%s', e)
        return []


def load_session(session_id: str) -> dict:
    """
    从 ClickHouse 加载一个完整会话，返回：
      {
        'chat_messages':  [...],   # 用于 Streamlit 展示
        'nl2sql_history': [...],   # 注入 NL2SQL Prompt
        'rag_history':    [...],   # 注入 RAG messages
      }
    """
    chat_messages  = []
    nl2sql_history = []
    rag_history    = []

    try:
        rows = _get_ch().query(f"""
            SELECT role, msg_type, content, sql_text, result_summary, sources
            FROM stream.chat_sessions
            WHERE session_id = '{session_id}'
            ORDER BY turn_index ASC
        """).result_rows
    except Exception as e:
        log.error('加载会话失败：%s', e)
        return {'chat_messages': chat_messages,
                'nl2sql_history': nl2sql_history,
                'rag_history': rag_history}

    for role, msg_type, content, sql_text, result_summary, sources in rows:
        if role == 'user':
            chat_messages.append({'role': 'user', 'content': content})
        else:
            if msg_type == 'nl2sql':
                # assistant 侧只恢复文字摘要（DataFrame 不持久化）
                chat_messages.append({
                    'role': 'assistant', 'type': 'nl2sql',
                    'sql': sql_text,
                    'insight': content,
                    'data': None,   # DataFrame 无法持久化，历史消息不展示图表
                    'error': None,
                })
            elif msg_type == 'rag':
                chat_messages.append({
                    'role': 'assistant', 'type': 'rag',
                    'content': content,
                    'sources': [s for s in sources.split(',') if s],
                })

        # 重建 NL2SQL 历史（仅 user+assistant nl2sql 配对）
        if role == 'user' and msg_type == 'nl2sql_q':
            # 占位，下一条 assistant 会填
            nl2sql_history.append({'question': content, 'sql': '', 'result_summary': ''})
        elif role == 'assistant' and msg_type == 'nl2sql' and nl2sql_history:
            nl2sql_history[-1]['sql'] = sql_text
            nl2sql_history[-1]['result_summary'] = result_summary

        # 重建 RAG 历史
        if role == 'user' and msg_type == 'rag_q':
            rag_history.append({'question': content, 'answer': ''})
        elif role == 'assistant' and msg_type == 'rag' and rag_history:
            rag_history[-1]['answer'] = content

    # 保留最近 N 轮
    return {
        'chat_messages':  chat_messages,
        'nl2sql_history': nl2sql_history[-5:],
        'rag_history':    rag_history[-8:],
    }
