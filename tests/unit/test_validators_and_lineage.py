# -*- coding: utf-8 -*-
"""Tests for utils/sql_validator.py"""
import pytest

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
        result = check_sql("SELECT created_at FROM orders")
        assert result is None
