# -*- coding: utf-8 -*-
"""ClickHouse 客户端单元测试（全部使用 mock，不建立真实连接）"""
import sys
import pytest
sys.path.insert(0, '/home/user/ai-data-warehouse')

try:
    from unittest.mock import MagicMock, patch
except ImportError:
    pytest.skip("unittest.mock 不可用", allow_module_level=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(host="ch-host", port=9000, user="default", password="secret"):
    """返回一个模拟 cfg 对象，包含 ClickHouse 连接属性。"""
    mock_cfg = MagicMock()
    mock_cfg.ch_host = host
    mock_cfg.ch_port = port
    mock_cfg.ch_user = user
    mock_cfg.ch_password = password
    return mock_cfg


# ---------------------------------------------------------------------------
# Tests for get_client()
# ---------------------------------------------------------------------------

class TestGetClient:
    """测试 get_client() 工厂函数"""

    def setup_method(self):
        try:
            from src.storage.clickhouse import client as ch_module  # noqa: F401
        except ImportError:
            pytest.skip("src.storage.clickhouse.client 不可用")

    # 1. get_client() 使用正确参数（host/port/user/password/timeouts）调用 clickhouse_connect
    def test_returns_client_with_correct_default_params(self):
        mock_cc = MagicMock()
        mock_cfg = _make_cfg()

        with patch.dict("sys.modules", {"clickhouse_connect": mock_cc}), \
             patch("src.storage.clickhouse.client.clickhouse_connect", mock_cc), \
             patch("src.storage.clickhouse.client._CH_AVAILABLE", True), \
             patch("src.storage.clickhouse.client.cfg", mock_cfg):
            from src.storage.clickhouse.client import get_client
            # reload to pick up patches applied above
            result = get_client()

        mock_cc.get_client.assert_called_once_with(
            host="ch-host",
            port=9000,
            username="default",
            password="secret",
            connect_timeout=10,
            send_receive_timeout=60,
        )
        assert result is mock_cc.get_client.return_value

    # 2. get_client() 正确透传自定义超时参数
    def test_passes_custom_timeout_params(self):
        mock_cc = MagicMock()
        mock_cfg = _make_cfg()

        with patch("src.storage.clickhouse.client.clickhouse_connect", mock_cc), \
             patch("src.storage.clickhouse.client._CH_AVAILABLE", True), \
             patch("src.storage.clickhouse.client.cfg", mock_cfg):
            from src.storage.clickhouse.client import get_client
            get_client(connect_timeout=30, send_receive_timeout=120)

        mock_cc.get_client.assert_called_once_with(
            host="ch-host",
            port=9000,
            username="default",
            password="secret",
            connect_timeout=30,
            send_receive_timeout=120,
        )

    # 3. 当 _CH_AVAILABLE=False 时 get_client() 抛出 ImportError
    def test_raises_import_error_when_ch_unavailable(self):
        with patch("src.storage.clickhouse.client._CH_AVAILABLE", False):
            from src.storage.clickhouse.client import get_client
            with pytest.raises(ImportError):
                get_client()


# ---------------------------------------------------------------------------
# Tests for ClickHouseClient
# ---------------------------------------------------------------------------

class TestClickHouseClient:
    """测试 ClickHouseClient 封装类"""

    def setup_method(self):
        try:
            from src.storage.clickhouse.client import ClickHouseClient
            self.ClickHouseClient = ClickHouseClient
        except ImportError:
            pytest.skip("src.storage.clickhouse.client 不可用")

    def _make_client_with_mock(self):
        """创建 ClickHouseClient 实例，并以 mock 替换内部 get_client()。"""
        mock_inner = MagicMock()
        ch_client = self.ClickHouseClient()
        with patch("src.storage.clickhouse.client.get_client", return_value=mock_inner):
            # 触发懒加载，确保 _client 被设置为 mock_inner
            _ = ch_client.client
        return ch_client, mock_inner

    # 4. 懒加载：__init__ 后 _client 为 None，首次访问 .client 后创建
    def test_lazy_init_client_is_none_initially(self):
        ch_client = self.ClickHouseClient()
        assert ch_client._client is None

    def test_lazy_init_client_created_on_first_access(self):
        mock_inner = MagicMock()
        ch_client = self.ClickHouseClient()

        with patch("src.storage.clickhouse.client.get_client", return_value=mock_inner) as mock_get:
            result = ch_client.client

        mock_get.assert_called_once()
        assert result is mock_inner
        assert ch_client._client is mock_inner

    def test_lazy_init_get_client_called_only_once(self):
        """连续两次访问 .client 只调用一次 get_client()。"""
        mock_inner = MagicMock()
        ch_client = self.ClickHouseClient()

        with patch("src.storage.clickhouse.client.get_client", return_value=mock_inner) as mock_get:
            _ = ch_client.client
            _ = ch_client.client

        mock_get.assert_called_once()

    # 5. query_df() 调用 client.query() 并返回 result_set
    def test_query_df_calls_query_and_returns_result_set(self):
        mock_result = MagicMock()
        mock_result.result_set = [("row1",), ("row2",)]

        mock_inner = MagicMock()
        mock_inner.query.return_value = mock_result

        ch_client = self.ClickHouseClient()
        ch_client._client = mock_inner  # 直接注入，绕过懒加载

        with patch("src.storage.clickhouse.client._PANDAS_AVAILABLE", True):
            result = ch_client.query_df("SELECT 1")

        mock_inner.query.assert_called_once_with("SELECT 1")
        assert result is mock_result.result_set

    def test_query_df_fallback_without_pandas(self):
        """pandas 不可用时返回 list[dict]。"""
        mock_result = MagicMock()
        mock_result.column_names = ["id", "name"]
        mock_result.result_set = [(1, "Alice"), (2, "Bob")]

        mock_inner = MagicMock()
        mock_inner.query.return_value = mock_result

        ch_client = self.ClickHouseClient()
        ch_client._client = mock_inner

        with patch("src.storage.clickhouse.client._PANDAS_AVAILABLE", False):
            result = ch_client.query_df("SELECT id, name FROM t")

        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    # 6. execute() DDL 路径：data=None 时调用 client.command(sql)
    def test_execute_ddl_calls_command(self):
        mock_inner = MagicMock()
        ch_client = self.ClickHouseClient()
        ch_client._client = mock_inner

        ch_client.execute("CREATE TABLE IF NOT EXISTS t (id Int32) ENGINE=Memory")

        mock_inner.command.assert_called_once_with(
            "CREATE TABLE IF NOT EXISTS t (id Int32) ENGINE=Memory"
        )
        mock_inner.insert.assert_not_called()

    # 7. execute() 写入路径：提供 data 时调用 client.insert(table, data, column_names=...)
    def test_execute_insert_calls_insert_with_data(self):
        mock_inner = MagicMock()
        ch_client = self.ClickHouseClient()
        ch_client._client = mock_inner

        rows = [[1, "Alice"], [2, "Bob"]]
        ch_client.execute("db.my_table", data=rows, column_names=["id", "name"])

        mock_inner.insert.assert_called_once_with(
            "db.my_table", rows, column_names=["id", "name"]
        )
        mock_inner.command.assert_not_called()

    def test_execute_insert_defaults_column_names_to_empty_list(self):
        """column_names=None 时应传入空列表。"""
        mock_inner = MagicMock()
        ch_client = self.ClickHouseClient()
        ch_client._client = mock_inner

        rows = [[42]]
        ch_client.execute("db.my_table", data=rows)

        mock_inner.insert.assert_called_once_with(
            "db.my_table", rows, column_names=[]
        )

    # 8. close() 关闭连接并将 _client 重置为 None
    def test_close_clears_client_to_none(self):
        mock_inner = MagicMock()
        ch_client = self.ClickHouseClient()
        ch_client._client = mock_inner

        ch_client.close()

        mock_inner.close.assert_called_once()
        assert ch_client._client is None

    def test_close_is_noop_when_already_none(self):
        """_client 已为 None 时 close() 不应抛出异常。"""
        ch_client = self.ClickHouseClient()
        assert ch_client._client is None
        ch_client.close()  # 不应抛出
        assert ch_client._client is None
