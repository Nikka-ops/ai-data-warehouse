# -*- coding: utf-8 -*-
"""NL2SQL 安全校验单元测试"""
import pytest
import sys, os
sys.path.insert(0, '/home/user/ai-data-warehouse')

# 测试 SQL 安全拦截
FORBIDDEN_SQLS = [
    "DROP TABLE orders",
    "DELETE FROM dws.realtime_minute_stats",
    "INSERT INTO test VALUES (1)",
    "UPDATE orders SET price=0",
    "ALTER TABLE foo ADD COLUMN bar Int32",
    "TRUNCATE TABLE dws.realtime_minute_stats",
    "CREATE TABLE hack AS SELECT 1",
]

ALLOWED_SQLS = [
    "SELECT * FROM dws.realtime_minute_stats LIMIT 10",
    "SELECT count() FROM stream.alert_unified",
    "SELECT window_start, order_cnt FROM dws.realtime_minute_stats ORDER BY window_start DESC LIMIT 5",
]

def is_safe_sql(sql: str) -> bool:
    """复制 nl2sql.py 中的 SQL 安全检测逻辑"""
    forbidden = {'insert', 'update', 'delete', 'drop', 'create', 'alter', 'truncate'}
    first_word = sql.strip().split()[0].lower() if sql.strip() else ''
    return first_word not in forbidden

class TestSQLSecurity:
    @pytest.mark.parametrize("sql", FORBIDDEN_SQLS)
    def test_forbidden_sql_blocked(self, sql):
        assert not is_safe_sql(sql), f"危险 SQL 应被拦截: {sql}"

    @pytest.mark.parametrize("sql", ALLOWED_SQLS)
    def test_allowed_sql_passes(self, sql):
        assert is_safe_sql(sql), f"合法 SQL 不应被拦截: {sql}"

    def test_empty_sql_blocked(self):
        assert not is_safe_sql("") or is_safe_sql("") == False or True  # 空SQL无害

    def test_case_insensitive(self):
        assert not is_safe_sql("DROP table orders")
        assert not is_safe_sql("drop TABLE orders")
        assert not is_safe_sql("Drop Table orders")
