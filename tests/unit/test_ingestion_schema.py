# -*- coding: utf-8 -*-
"""
Unit tests for ingestion schema modules:
  - src/ingestion/schema/avro_serde.py
  - src/ingestion/schema/schema_registry.py
"""

import sys
import json
from unittest.mock import MagicMock, patch

sys.path.insert(0, '/home/user/ai-data-warehouse')

try:
    from src.ingestion.schema.avro_serde import (
        serialize,
        deserialize,
        ORDER_SCHEMA_STR,
    )
    import src.ingestion.schema.avro_serde as avro_serde_module
except ImportError as exc:
    pytest_skip_avro = str(exc)
    serialize = None  # type: ignore[assignment]
    deserialize = None  # type: ignore[assignment]
    ORDER_SCHEMA_STR = None  # type: ignore[assignment]
    avro_serde_module = None  # type: ignore[assignment]
else:
    pytest_skip_avro = None

try:
    from src.ingestion.schema.schema_registry import SchemaRegistryClient
except ImportError as exc:
    pytest_skip_registry = str(exc)
    SchemaRegistryClient = None  # type: ignore[assignment]
else:
    pytest_skip_registry = None

try:
    import pytest
except ImportError as exc:
    raise SystemExit("pytest is required to run these tests") from exc


# ── Shared fixture ─────────────────────────────────────────────────────────────

ORDER_EVENT = {
    "order_id": "ord-001",
    "customer_id": "cust-42",
    "price": 19.99,
    "event_time": 1717286400000,  # millis since epoch
}


# ─────────────────────────────────────────────────────────────────────────────
# Part 1: AvroSerde tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAvroSerde:
    """Tests for src/ingestion/schema/avro_serde.py"""

    def setup_method(self):
        if pytest_skip_avro:
            pytest.skip(f"avro_serde import failed: {pytest_skip_avro}")

    # ── Test 1: JSON fallback roundtrip ────────────────────────────────────────

    def test_json_fallback_roundtrip(self):
        """serialize + deserialize roundtrip works with JSON fallback backend."""
        with patch.object(avro_serde_module, "_BACKEND", None):
            raw = serialize(ORDER_EVENT, ORDER_SCHEMA_STR)
            result = deserialize(raw, ORDER_SCHEMA_STR)
        assert result == ORDER_EVENT

    # ── Test 2: serialize returns bytes ───────────────────────────────────────

    def test_json_fallback_serialize_returns_bytes(self):
        """serialize() returns bytes when _BACKEND is None."""
        with patch.object(avro_serde_module, "_BACKEND", None):
            raw = serialize(ORDER_EVENT, ORDER_SCHEMA_STR)
        assert isinstance(raw, bytes)

    # ── Test 3: deserialize recovers original dict ────────────────────────────

    def test_json_fallback_deserialize_recovers_original(self):
        """deserialize(serialize(record)) returns the original dict under JSON fallback."""
        with patch.object(avro_serde_module, "_BACKEND", None):
            raw = serialize(ORDER_EVENT, ORDER_SCHEMA_STR)
            result = deserialize(raw, ORDER_SCHEMA_STR)
        assert result["order_id"] == ORDER_EVENT["order_id"]
        assert result["customer_id"] == ORDER_EVENT["customer_id"]
        assert result["price"] == pytest.approx(ORDER_EVENT["price"])
        assert result["event_time"] == ORDER_EVENT["event_time"]

    # ── Test 4: fastavro full avro roundtrip ──────────────────────────────────

    def test_fastavro_roundtrip(self):
        """Full Avro roundtrip via fastavro with ORDER_SCHEMA_STR."""
        try:
            import fastavro  # noqa: F401
        except ImportError:
            pytest.skip("fastavro is not installed")

        # Use the real fastavro backend (don't patch _BACKEND)
        raw = serialize(ORDER_EVENT, ORDER_SCHEMA_STR)
        result = deserialize(raw, ORDER_SCHEMA_STR)

        assert isinstance(raw, bytes)
        assert result["order_id"] == ORDER_EVENT["order_id"]
        assert result["customer_id"] == ORDER_EVENT["customer_id"]
        assert result["price"] == pytest.approx(ORDER_EVENT["price"])
        assert result["event_time"] == ORDER_EVENT["event_time"]


# ─────────────────────────────────────────────────────────────────────────────
# Part 2: SchemaRegistry tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaRegistry:
    """Tests for src/ingestion/schema/schema_registry.py"""

    def setup_method(self):
        if pytest_skip_registry:
            pytest.skip(f"schema_registry import failed: {pytest_skip_registry}")
        self.client = SchemaRegistryClient(url="http://localhost:8081")
        self.schema_str = ORDER_SCHEMA_STR or json.dumps({"type": "record", "name": "OrderEvent"})

    def _make_urlopen_mock(self, response_body: dict) -> MagicMock:
        """Return a mock context manager simulating urlopen returning response_body."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_body).encode()
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_resp
        mock_cm.__exit__.return_value = False
        return mock_cm

    # ── Test 5: register() POSTs and returns id ───────────────────────────────

    def test_register_sends_post_and_returns_id(self):
        """register() POSTs to /subjects/{subject}/versions and returns the schema id."""
        mock_cm = self._make_urlopen_mock({"id": 42})

        with patch("urllib.request.urlopen", return_value=mock_cm) as mock_urlopen:
            result = self.client.register("orders-value", self.schema_str)

        assert result == 42

        # Verify the Request object was built with the correct URL and method
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        assert "/subjects/orders-value/versions" in req.full_url
        assert req.method == "POST"

    # ── Test 6: get_latest() GETs and returns full response dict ──────────────

    def test_get_latest_returns_response_dict(self):
        """get_latest() GETs /subjects/{subject}/versions/latest and returns the dict."""
        expected = {
            "subject": "orders-value",
            "version": 3,
            "id": 42,
            "schema": self.schema_str,
        }
        mock_cm = self._make_urlopen_mock(expected)

        with patch("urllib.request.urlopen", return_value=mock_cm) as mock_urlopen:
            result = self.client.get_latest("orders-value")

        assert result == expected

        req = mock_urlopen.call_args[0][0]
        assert "/subjects/orders-value/versions/latest" in req.full_url
        assert req.method == "GET"

    # ── Test 7: get_by_id() GETs and returns schema string ───────────────────

    def test_get_by_id_returns_schema_string(self):
        """get_by_id() GETs /schemas/ids/{id} and returns the schema string."""
        mock_cm = self._make_urlopen_mock({"schema": self.schema_str})

        with patch("urllib.request.urlopen", return_value=mock_cm) as mock_urlopen:
            result = self.client.get_by_id(42)

        assert result == self.schema_str

        req = mock_urlopen.call_args[0][0]
        assert "/schemas/ids/42" in req.full_url
        assert req.method == "GET"

    # ── Test 8: HTTP errors from urlopen are not swallowed ────────────────────

    def test_http_error_is_not_swallowed(self):
        """An HTTPError from urlopen propagates out of the client without being swallowed."""
        import urllib.error

        http_error = urllib.error.HTTPError(
            url="http://localhost:8081/subjects/bad/versions",
            code=409,
            msg="Conflict",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )

        with patch("urllib.request.urlopen", side_effect=http_error):
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                self.client.register("bad-subject", self.schema_str)

        assert exc_info.value.code == 409
