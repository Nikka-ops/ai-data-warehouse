# -*- coding: utf-8 -*-
"""
ClickHouse 客户端封装，提供简单工厂和带懒加载连接池的封装类
"""

from __future__ import annotations
from typing import Any

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False  # pandas 不可用时降级

try:
    import clickhouse_connect
    _CH_AVAILABLE = True
except ImportError:
    clickhouse_connect = None  # type: ignore[assignment]
    _CH_AVAILABLE = False      # 依赖缺失时不崩溃

from src.common.config import cfg
from src.common.utils import get_logger

log = get_logger("storage.clickhouse")


def get_client():
    """获取 ClickHouse 客户端（简单工厂）"""
    if not _CH_AVAILABLE:
        raise ImportError("clickhouse_connect 未安装，请执行 pip install clickhouse-connect")
    return clickhouse_connect.get_client(
        host=cfg.ch_host,
        port=cfg.ch_port,
        username=cfg.ch_user,
        password=cfg.ch_password,
    )


class ClickHouseClient:
    """带懒加载连接和重试的 ClickHouse 客户端封装"""

    def __init__(self) -> None:
        self._client = None  # 延迟初始化，首次使用时建立连接

    @property
    def client(self):
        """懒加载：首次访问时才建立连接"""
        if self._client is None:
            log.info("初始化 ClickHouse 连接 %s:%s", cfg.ch_host, cfg.ch_port)
            self._client = get_client()
        return self._client

    def query_df(self, sql: str):
        """执行查询，返回 pandas DataFrame；pandas 不可用时返回 list[dict]"""
        log.debug("执行查询：%s", sql[:200])
        result = self.client.query(sql)
        if _PANDAS_AVAILABLE:
            return result.result_set  # clickhouse_connect 原生支持 DataFrame
        # 降级为字典列表
        cols = result.column_names
        return [dict(zip(cols, row)) for row in result.result_set]

    def execute(self, sql: str, data: Any = None) -> None:
        """执行写入或 DDL 语句"""
        log.debug("执行写入/DDL：%s", sql[:200])
        if data is not None:
            self.client.insert(sql, data)
        else:
            self.client.command(sql)

    def close(self) -> None:
        """关闭连接并清理资源"""
        if self._client is not None:
            self._client.close()
            self._client = None
            log.info("ClickHouse 连接已关闭")
