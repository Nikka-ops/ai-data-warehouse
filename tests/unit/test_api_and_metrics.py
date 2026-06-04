# -*- coding: utf-8 -*-
"""Unit tests for src/api/rest/schemas.py"""

import pytest

try:
    from pydantic import ValidationError
    from src.api.rest.schemas import QueryRequest, QueryResponse
    _schemas_available = True
except ImportError:
    _schemas_available = False


@pytest.mark.skipif(not _schemas_available, reason="src.api.rest.schemas not importable")
class TestApiSchemas:
    def test_query_request_valid_question(self):
        req = QueryRequest(question="今日 GMV 是多少？")
        assert req.question == "今日 GMV 是多少？"

    def test_query_request_defaults(self):
        req = QueryRequest(question="show orders")
        assert req.max_rows == 100
        assert req.use_cache is True

    def test_query_request_rejects_max_rows_over_1000(self):
        with pytest.raises(ValidationError):
            QueryRequest(question="show orders", max_rows=1001)

    def test_query_response_constructs_correctly(self):
        resp = QueryResponse(
            sql="SELECT 1",
            data=[{"col": "val"}],
            row_count=1,
            elapsed_ms=42.5,
        )
        assert resp.sql == "SELECT 1"
        assert resp.row_count == 1
        assert resp.insight == ""
        assert resp.confidence == 1.0
