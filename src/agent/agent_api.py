# -*- coding: utf-8 -*-
"""Agent 快速调用接口"""
from src.agent.agents.sql_agent import SQLAgent
from src.agent.agents.alert_agent import AlertAgent
from src.agent.agents.rag_agent import RAGAgent

_SQL_AGENT = None
_ALERT_AGENT = None
_RAG_AGENT = None

def get_sql_agent() -> SQLAgent:
    global _SQL_AGENT
    if _SQL_AGENT is None:
        _SQL_AGENT = SQLAgent()
    return _SQL_AGENT

def query(question: str) -> dict:
    """NL2SQL 查询"""
    return get_sql_agent().run(question)

def diagnose_alert(alert_detail: str) -> dict:
    """告警诊断"""
    global _ALERT_AGENT
    if _ALERT_AGENT is None:
        _ALERT_AGENT = AlertAgent()
    return _ALERT_AGENT.run(alert_detail)
