# -*- coding: utf-8 -*-
"""SQL Agent：NL2SQL + Self-RAG 查询"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
from src.agent.core.base_agent import BaseAgent

class SQLAgent(BaseAgent):
    name = "sql"

    def run(self, goal: str) -> dict:
        from ai_layer.nl2sql import nl2sql_query
        result = nl2sql_query(goal)
        return self._wrap_result(str(result.get("insight", "")), [result])
