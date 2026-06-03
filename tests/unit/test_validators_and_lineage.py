# -*- coding: utf-8 -*-
"""
Tests for:
  - utils/sql_validator.py
  - src/lineage/analyzer/ (FreshnessTracker, ImpactAnalyzer)
  - src/monitoring/alerts/notifiers/ (dingtalk, feishu, slack)
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta


# ─────────────────────────────────────────────────────────────────────────────
# Part 1 — utils/sql_validator.py
# ─────────────────────────────────────────────────────────────────────────────

try:
    from utils.sql_validator import validate_sql, check_sql
    _validator_ok = True
except ImportError:
    _validator_ok = False


@pytest.mark.skipif(not _validator_ok, reason="utils.sql_validator not importable")
class TestSqlValidator:
    def test_validate_sql_allows_select(self):
        validate_sql("SELECT count() FROM dws.realtime_minute_stats")

    def test_validate_sql_allows_with(self):
        validate_sql("WITH t AS (SELECT 1) SELECT * FROM t")

    def test_validate_sql_raises_on_drop(self):
        with pytest.raises(ValueError, match="DROP"):
            validate_sql("DROP TABLE foo")

    def test_validate_sql_raises_on_insert(self):
        with pytest.raises(ValueError, match="INSERT"):
            validate_sql("INSERT INTO t VALUES (1)")

    def test_validate_sql_raises_on_update(self):
        with pytest.raises(ValueError, match="UPDATE"):
            validate_sql("UPDATE t SET x=1")

    def test_validate_sql_raises_on_delete(self):
        with pytest.raises(ValueError, match="DELETE"):
            validate_sql("DELETE FROM t")

    def test_validate_sql_raises_on_truncate(self):
        with pytest.raises(ValueError, match="TRUNCATE"):
            validate_sql("TRUNCATE TABLE t")

    def test_validate_sql_raises_on_non_select(self):
        with pytest.raises(ValueError):
            validate_sql("SHOW TABLES")

    def test_validate_sql_case_insensitive(self):
        with pytest.raises(ValueError):
            validate_sql("drop table orders")

    def test_check_sql_returns_none_for_valid(self):
        assert check_sql("SELECT 1") is None

    def test_check_sql_returns_string_for_invalid(self):
        result = check_sql("DROP TABLE foo")
        assert result is not None
        assert isinstance(result, str)
        assert "DROP" in result

    def test_check_sql_keyword_in_identifier_not_blocked(self):
        # "CREATED_AT" contains no standalone \bCREATE\b — should pass
        result = check_sql("SELECT created_at FROM orders")
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# Part 2 — src/lineage/analyzer/freshness_tracker.py
# ─────────────────────────────────────────────────────────────────────────────

try:
    from src.lineage.analyzer.freshness_tracker import FreshnessTracker
    _freshness_ok = True
except ImportError:
    _freshness_ok = False


@pytest.mark.skipif(not _freshness_ok, reason="FreshnessTracker not importable")
class TestFreshnessTracker:
    def setup_method(self):
        self.ch = MagicMock()
        self.tracker = FreshnessTracker(self.ch)

    def test_check_fresh_table(self):
        # last_time is very recent → stale=False
        self.ch.query.return_value.result_rows = [[datetime.now()]]
        result = self.tracker.check("dws.realtime_minute_stats")
        assert result["stale"] is False
        assert result["table"] == "dws.realtime_minute_stats"

    def test_check_stale_table(self):
        # last_time is 10 hours ago → stale for a 2-minute threshold table
        self.ch.query.return_value.result_rows = [[datetime.now() - timedelta(hours=10)]]
        result = self.tracker.check("dws.realtime_minute_stats")
        assert result["stale"] is True

    def test_check_no_data_is_stale(self):
        # empty result → stale
        self.ch.query.return_value.result_rows = []
        result = self.tracker.check("dws.realtime_minute_stats")
        assert result["stale"] is True

    def test_check_unknown_table_uses_24h_default(self):
        # Unknown table uses 24h threshold; recent data → fresh
        self.ch.query.return_value.result_rows = [[datetime.now() - timedelta(hours=1)]]
        result = self.tracker.check("some.unknown_table")
        assert result["stale"] is False

    def test_check_query_exception_marks_stale(self):
        self.ch.query.side_effect = RuntimeError("connection lost")
        result = self.tracker.check("dws.realtime_minute_stats")
        assert result["stale"] is True
        assert "error" in result

    def test_check_all_returns_list(self):
        self.ch.query.return_value.result_rows = [[datetime.now()]]
        results = self.tracker.check_all()
        assert isinstance(results, list)
        assert len(results) == len(FreshnessTracker.FRESHNESS_THRESHOLDS)


# ─────────────────────────────────────────────────────────────────────────────
# Part 3 — src/lineage/analyzer/impact_analyzer.py
# ─────────────────────────────────────────────────────────────────────────────

try:
    from src.lineage.analyzer.impact_analyzer import ImpactAnalyzer
    from src.lineage.graph.lineage_graph import LineageGraph
    from src.lineage.graph.node import LineageNode, NodeType
    from src.lineage.graph.edge import LineageEdge, EdgeType
    _impact_ok = True
except ImportError:
    _impact_ok = False


@pytest.mark.skipif(not _impact_ok, reason="ImpactAnalyzer not importable")
class TestImpactAnalyzer:
    def setup_method(self):
        self.graph = LineageGraph()
        nodes = [
            LineageNode("kafka",  "kafka.orders",                NodeType.KAFKA_TOPIC),
            LineageNode("ods",    "ods.orders_stream",           NodeType.TABLE),
            LineageNode("dwd",    "dwd.realtime_order_detail",   NodeType.TABLE),
            LineageNode("dws",    "dws.realtime_minute_stats",   NodeType.TABLE),
            LineageNode("ads",    "ads.realtime_hourly",         NodeType.VIEW),
        ]
        for n in nodes:
            self.graph.add_node(n)
        edges = [
            LineageEdge("kafka", "ods", EdgeType.WRITES_TO),
            LineageEdge("ods",   "dwd", EdgeType.DERIVED_FROM),
            LineageEdge("dwd",   "dws", EdgeType.DERIVED_FROM),
            LineageEdge("dws",   "ads", EdgeType.DERIVED_FROM),
        ]
        for e in edges:
            self.graph.add_edge(e)
        self.analyzer = ImpactAnalyzer(self.graph)

    def test_analyze_returns_expected_keys(self):
        result = self.analyzer.analyze("ods")
        assert "affected_table" in result
        assert "downstream_tables" in result
        assert "impact_score" in result
        assert "severity" in result

    def test_analyze_kafka_has_high_downstream_count(self):
        result = self.analyzer.analyze("kafka")
        assert result["downstream_count"] >= 3

    def test_analyze_leaf_has_no_downstream(self):
        result = self.analyzer.analyze("ads")
        assert result["downstream_count"] == 0

    def test_severity_is_valid_enum(self):
        result = self.analyzer.analyze("kafka")
        assert result["severity"] in ("P1", "P2", "P3")

    def test_analyze_upstream_count(self):
        result = self.analyzer.analyze("dws")
        assert result["upstream_count"] >= 2  # ods and dwd upstream


# ─────────────────────────────────────────────────────────────────────────────
# Part 4 — src/monitoring/alerts/notifiers/ (dingtalk, feishu, slack)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from src.monitoring.alerts.notifiers.dingtalk import send_dingtalk
    from src.monitoring.alerts.notifiers.feishu import send_feishu
    from src.monitoring.alerts.notifiers.slack import send_slack
    _notifiers_ok = True
except ImportError:
    _notifiers_ok = False

def _mock_urlopen_success():
    mock_resp = MagicMock()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


@pytest.mark.skipif(not _notifiers_ok, reason="notifiers not importable")
class TestNotifiers:
    def test_dingtalk_success(self):
        with patch("src.monitoring.alerts.notifiers.dingtalk.urllib.request.urlopen",
                   return_value=_mock_urlopen_success()):
            result = send_dingtalk("http://fake", "Test", "body", "P1")
        assert result is True

    def test_dingtalk_failure_returns_false(self):
        with patch("src.monitoring.alerts.notifiers.dingtalk.urllib.request.urlopen",
                   side_effect=OSError("network")):
            result = send_dingtalk("http://fake", "Test", "body", "P2")
        assert result is False

    def test_feishu_success(self):
        with patch("src.monitoring.alerts.notifiers.feishu.urllib.request.urlopen",
                   return_value=_mock_urlopen_success()):
            result = send_feishu("http://fake", "Test", "body", "P2")
        assert result is True

    def test_feishu_failure_returns_false(self):
        with patch("src.monitoring.alerts.notifiers.feishu.urllib.request.urlopen",
                   side_effect=OSError("network")):
            result = send_feishu("http://fake", "Test", "body")
        assert result is False

    def test_slack_success(self):
        with patch("src.monitoring.alerts.notifiers.slack.urllib.request.urlopen",
                   return_value=_mock_urlopen_success()):
            result = send_slack("http://fake", "Test", "body", "P3")
        assert result is True

    def test_slack_failure_returns_false(self):
        with patch("src.monitoring.alerts.notifiers.slack.urllib.request.urlopen",
                   side_effect=OSError("network")):
            result = send_slack("http://fake", "Test", "body")
        assert result is False

    def test_dingtalk_p1_severity_in_payload(self):
        captured = {}
        def fake_urlopen(req, timeout=None):
            import json
            captured["body"] = json.loads(req.data.decode())
            return _mock_urlopen_success()
        with patch("src.monitoring.alerts.notifiers.dingtalk.urllib.request.urlopen", fake_urlopen):
            send_dingtalk("http://fake", "Alert", "detail", "P1")
        assert "P1" in str(captured["body"])
