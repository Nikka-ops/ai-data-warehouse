# -*- coding: utf-8 -*-
"""Agent 快速调用接口"""
from src.agent.agents.sql_agent import SQLAgent
from src.agent.agents.rag_agent import RAGAgent

_SQL_AGENT = None
_RAG_AGENT = None

def get_sql_agent() -> SQLAgent:
    global _SQL_AGENT
    if _SQL_AGENT is None:
        _SQL_AGENT = SQLAgent()
    return _SQL_AGENT

def get_rag_agent() -> RAGAgent:
    global _RAG_AGENT
    if _RAG_AGENT is None:
        _RAG_AGENT = RAGAgent()
    return _RAG_AGENT

def query(question: str) -> dict:
    """NL2SQL 查询"""
    return get_sql_agent().run(question)

def rag_query(question: str) -> dict:
    """RAG 知识库查询"""
    return get_rag_agent().run(question)
