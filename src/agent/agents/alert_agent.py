# -*- coding: utf-8 -*-
"""Alert Agent：告警检测 + 自动诊断 + 修复"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
from src.agent.core.base_agent import BaseAgent

class AlertAgent(BaseAgent):
    name = "alert"

    def run(self, goal: str) -> dict:
        from ai_layer.alert_engine.aggregator import AlertAggregator
        import clickhouse_connect
        from src.common.config import cfg
        ch = clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port,
            username=cfg.ch_user, password=cfg.ch_password)
        agg = AlertAggregator()
        alerts = agg.run_all_detectors(ch)
        summary = f"检测到 {len(alerts)} 条告警：" + ", ".join(a.title for a in alerts[:5])
        return self._wrap_result(summary, [{"alerts": [a.title for a in alerts]}])
