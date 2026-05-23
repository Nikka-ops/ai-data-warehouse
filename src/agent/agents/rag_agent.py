# -*- coding: utf-8 -*-
"""RAG Agent：知识库检索 + 生成回答"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
from src.agent.core.base_agent import BaseAgent

class RAGAgent(BaseAgent):
    name = "rag"

    def run(self, goal: str) -> dict:
        from ai_layer.rag_engine import rag_query
        result = rag_query(goal)
        return self._wrap_result(result.get("answer", ""), [result])
