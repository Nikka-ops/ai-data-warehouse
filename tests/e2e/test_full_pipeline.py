# -*- coding: utf-8 -*-
"""端到端流水线测试"""
import pytest
import os

pytestmark = pytest.mark.skipif(
    os.getenv("E2E_TEST") != "1",
    reason="E2E 测试需要设置 E2E_TEST=1"
)

class TestFullPipeline:
    def test_nl2sql_returns_result(self):
        """NL2SQL 完整链路：问题 → SQL → 结果 → 洞察"""
        import sys
        sys.path.insert(0, '/home/user/ai-data-warehouse')
        from ai_layer.nl2sql import nl2sql_query
        result = nl2sql_query("当前最新5个分钟的订单量是多少？")
        assert "sql" in result
        assert result.get("sql", "").strip().upper().startswith("SELECT")

    def test_alert_engine_detects_alerts(self):
        """告警引擎能检测到告警（不要求有真实告警）"""
        import sys
        sys.path.insert(0, '/home/user/ai-data-warehouse')
        try:
            import clickhouse_connect
            from src.common.config import cfg
            ch = clickhouse_connect.get_client(
                host=cfg.ch_host, port=cfg.ch_port,
                username=cfg.ch_user, password=cfg.ch_password)
            from ai_layer.alert_engine.aggregator import AlertAggregator
            agg = AlertAggregator()
            alerts = agg.run_all_detectors(ch)
            assert isinstance(alerts, list)  # 无论有没有告警，返回值应是列表
        except Exception as e:
            pytest.fail(f"告警引擎运行失败: {e}")
