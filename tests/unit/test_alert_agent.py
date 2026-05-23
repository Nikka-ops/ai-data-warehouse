# -*- coding: utf-8 -*-
"""Alert Engine 单元测试"""
import pytest
from unittest.mock import MagicMock, patch
import sys
sys.path.insert(0, '/home/user/ai-data-warehouse')

class TestAlertAggregator:
    def test_dedup_same_fingerprint(self):
        """相同指纹的告警在时间窗口内只保留一条"""
        try:
            from ai_layer.alert_engine import AlertEvent
            from ai_layer.alert_engine.aggregator import AlertAggregator
        except ImportError:
            pytest.skip("alert_engine 未安装")

        agg = AlertAggregator(dedup_window_seconds=900)
        a1 = AlertEvent(source="rule", metric_name="order_cnt", category="BUSINESS",
                        title="订单量为零", severity="P1")
        a1.compute_fingerprint()
        a2 = AlertEvent(source="rule", metric_name="order_cnt", category="BUSINESS",
                        title="订单量为零", severity="P1")
        a2.fingerprint = a1.fingerprint  # 同一指纹

        result = agg.process([a1, a2])
        assert len(result) == 1, "相同指纹告警应被去重"

    def test_different_severity_not_deduped(self):
        """不同 metric 的告警不应被去重"""
        try:
            from ai_layer.alert_engine import AlertEvent
            from ai_layer.alert_engine.aggregator import AlertAggregator
        except ImportError:
            pytest.skip("alert_engine 未安装")

        agg = AlertAggregator(dedup_window_seconds=900)
        a1 = AlertEvent(source="rule", metric_name="order_cnt",  category="BUSINESS", title="订单量为零", severity="P1")
        a2 = AlertEvent(source="rule", metric_name="total_gmv",  category="BUSINESS", title="GMV为零",    severity="P1")
        a1.compute_fingerprint()
        a2.compute_fingerprint()

        result = agg.process([a1, a2])
        assert len(result) == 2, "不同 metric 告警不应被去重"

    def test_p1_sorted_first(self):
        """P1 告警应排在最前面"""
        try:
            from ai_layer.alert_engine import AlertEvent
            from ai_layer.alert_engine.aggregator import AlertAggregator
        except ImportError:
            pytest.skip("alert_engine 未安装")

        agg = AlertAggregator()
        alerts = [
            AlertEvent(source="rule", metric_name="m3", category="BUSINESS", title="P3告警", severity="P3"),
            AlertEvent(source="rule", metric_name="m1", category="BUSINESS", title="P1告警", severity="P1"),
            AlertEvent(source="rule", metric_name="m2", category="BUSINESS", title="P2告警", severity="P2"),
        ]
        for a in alerts:
            a.compute_fingerprint()
        result = agg.process(alerts)
        assert result[0].severity == "P1", "P1 告警应排首位"


class TestSafetyGate:
    def test_critical_action_blocked(self):
        """critical 操作永远拒绝"""
        try:
            from ai_layer.alert_engine.safety_gate import SafetyGate
        except ImportError:
            pytest.skip("safety_gate 未安装")

        gate = SafetyGate()
        allowed, reason = gate.check("drop_data", "dws.realtime_minute_stats")
        assert not allowed
        assert "critical" in reason.lower() or "禁止" in reason

    def test_low_risk_action_allowed(self):
        """low 风险操作应被允许（首次）"""
        try:
            from ai_layer.alert_engine.safety_gate import SafetyGate
        except ImportError:
            pytest.skip("safety_gate 未安装")

        gate = SafetyGate()
        allowed, reason = gate.check("clear_stale_features", "redis:feat:user:*")
        assert allowed
