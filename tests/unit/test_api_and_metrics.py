# -*- coding: utf-8 -*-
"""Unit tests for src/api/rest/schemas.py and src/monitoring/metrics/business_metrics.py"""

import pytest
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Part 1 — Pydantic schema validation
# ---------------------------------------------------------------------------

try:
    from pydantic import ValidationError
    from src.api.rest.schemas import (
        QueryRequest,
        QueryResponse,
        AlertRequest,
        AlertResponse,
        LineageRequest,
        FeatureRequest,
    )
    _schemas_available = True
except ImportError:
    _schemas_available = False


@pytest.mark.skipif(not _schemas_available, reason="src.api.rest.schemas not importable")
class TestApiSchemas:
    # ------------------------------------------------------------------
    # QueryRequest
    # ------------------------------------------------------------------

    def test_query_request_valid_question(self):
        """QueryRequest accepts a valid question string."""
        req = QueryRequest(question="What is today's GMV?")
        assert req.question == "What is today's GMV?"

    def test_query_request_defaults(self):
        """QueryRequest has default max_rows=100 and use_cache=True."""
        req = QueryRequest(question="show orders")
        assert req.max_rows == 100
        assert req.use_cache is True

    def test_query_request_rejects_max_rows_over_1000(self):
        """QueryRequest raises ValidationError when max_rows > 1000."""
        with pytest.raises(ValidationError):
            QueryRequest(question="show orders", max_rows=1001)

    # ------------------------------------------------------------------
    # AlertRequest
    # ------------------------------------------------------------------

    def test_alert_request_defaults(self):
        """AlertRequest has default severity='P3' and detail=''."""
        req = AlertRequest(source="monitor", title="High latency")
        assert req.severity == "P3"
        assert req.detail == ""

    # ------------------------------------------------------------------
    # LineageRequest
    # ------------------------------------------------------------------

    def test_lineage_request_defaults(self):
        """LineageRequest has default direction='both' and depth=3."""
        req = LineageRequest(table_name="dws.orders")
        assert req.direction == "both"
        assert req.depth == 3

    def test_lineage_request_rejects_depth_over_10(self):
        """LineageRequest raises ValidationError when depth > 10."""
        with pytest.raises(ValidationError):
            LineageRequest(table_name="dws.orders", depth=11)

    # ------------------------------------------------------------------
    # FeatureRequest
    # ------------------------------------------------------------------

    def test_feature_request_feature_names_defaults_to_empty_list(self):
        """FeatureRequest feature_names defaults to an empty list."""
        req = FeatureRequest(entity_type="user", entity_id="u_001")
        assert req.feature_names == []

    # ------------------------------------------------------------------
    # QueryResponse
    # ------------------------------------------------------------------

    def test_query_response_constructs_correctly(self):
        """QueryResponse constructs correctly with all required fields."""
        resp = QueryResponse(
            sql="SELECT 1",
            data=[{"col": "val"}],
            row_count=1,
            elapsed_ms=42.5,
        )
        assert resp.sql == "SELECT 1"
        assert resp.data == [{"col": "val"}]
        assert resp.row_count == 1
        assert resp.elapsed_ms == 42.5
        # optional fields carry their defaults
        assert resp.insight == ""
        assert resp.confidence == 1.0

    # ------------------------------------------------------------------
    # AlertResponse
    # ------------------------------------------------------------------

    def test_alert_response_constructs_correctly(self):
        """AlertResponse constructs correctly with all required fields."""
        resp = AlertResponse(
            alert_id="alert-123",
            diagnosis="Disk I/O spike",
            actions=["scale_up", "notify_oncall"],
            escalated=False,
        )
        assert resp.alert_id == "alert-123"
        assert resp.diagnosis == "Disk I/O spike"
        assert resp.actions == ["scale_up", "notify_oncall"]
        assert resp.escalated is False


# ---------------------------------------------------------------------------
# Part 2 — BusinessMetricsCollector
# ---------------------------------------------------------------------------

try:
    from src.monitoring.metrics.business_metrics import BusinessMetricsCollector
    _metrics_available = True
except ImportError:
    _metrics_available = False


@pytest.mark.skipif(not _metrics_available, reason="src.monitoring.metrics.business_metrics not importable")
class TestBusinessMetrics:
    # ------------------------------------------------------------------
    # collect_gmv
    # ------------------------------------------------------------------

    def test_collect_gmv_returns_float_from_mock(self):
        """collect_gmv() returns a float parsed from the ClickHouse result."""
        ch = MagicMock()
        ch.query.return_value.result_rows = [(123456.78,)]
        collector = BusinessMetricsCollector(ch)
        result = collector.collect_gmv()
        assert isinstance(result, float)
        assert result == pytest.approx(123456.78)

    def test_collect_gmv_returns_zero_when_empty(self):
        """collect_gmv() returns 0.0 when result_rows is empty."""
        ch = MagicMock()
        ch.query.return_value.result_rows = []
        collector = BusinessMetricsCollector(ch)
        assert collector.collect_gmv() == 0.0

    def test_collect_gmv_returns_zero_on_exception(self):
        """collect_gmv() returns 0.0 when ClickHouse raises an exception."""
        ch = MagicMock()
        ch.query.side_effect = RuntimeError("connection refused")
        collector = BusinessMetricsCollector(ch)
        assert collector.collect_gmv() == 0.0

    # ------------------------------------------------------------------
    # collect_order_count
    # ------------------------------------------------------------------

    def test_collect_order_count_returns_int_from_mock(self):
        """collect_order_count() returns an int parsed from the ClickHouse result."""
        ch = MagicMock()
        ch.query.return_value.result_rows = [(9876,)]
        collector = BusinessMetricsCollector(ch)
        result = collector.collect_order_count()
        assert isinstance(result, int)
        assert result == 9876

    def test_collect_order_count_returns_zero_when_empty(self):
        """collect_order_count() returns 0 when result_rows is empty."""
        ch = MagicMock()
        ch.query.return_value.result_rows = []
        collector = BusinessMetricsCollector(ch)
        assert collector.collect_order_count() == 0

    # ------------------------------------------------------------------
    # collect_all
    # ------------------------------------------------------------------

    def test_collect_all_returns_expected_keys(self):
        """collect_all() returns a dict with keys 'gmv', 'order_count', 'collected_at'."""
        ch = MagicMock()
        ch.query.return_value.result_rows = [(0,)]
        collector = BusinessMetricsCollector(ch)
        result = collector.collect_all()
        assert isinstance(result, dict)
        assert "gmv" in result
        assert "order_count" in result
        assert "collected_at" in result

    def test_collect_all_values_match_individual_collectors(self):
        """collect_all() gmv and order_count match values from individual collectors."""
        ch = MagicMock()

        # First call → collect_gmv (SELECT sum(total_gmv)…)
        # Second call → collect_order_count (SELECT sum(order_cnt)…)
        # collect_all calls each in turn, so we use side_effect to sequence results.
        gmv_result = MagicMock()
        gmv_result.result_rows = [(5000.50,)]
        order_result = MagicMock()
        order_result.result_rows = [(300,)]

        ch.query.side_effect = [gmv_result, order_result]
        collector = BusinessMetricsCollector(ch)
        all_metrics = collector.collect_all()

        # Reset and replay for individual calls
        ch.query.side_effect = [gmv_result, order_result]
        gmv_result.result_rows = [(5000.50,)]
        order_result.result_rows = [(300,)]

        ch2 = MagicMock()
        gmv_result2 = MagicMock()
        gmv_result2.result_rows = [(5000.50,)]
        order_result2 = MagicMock()
        order_result2.result_rows = [(300,)]
        ch2.query.side_effect = [gmv_result2, order_result2]

        collector2 = BusinessMetricsCollector(ch2)
        expected_gmv = collector2.collect_gmv()
        expected_order = collector2.collect_order_count()

        assert all_metrics["gmv"] == pytest.approx(expected_gmv)
        assert all_metrics["order_count"] == expected_order
