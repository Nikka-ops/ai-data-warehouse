# -*- coding: utf-8 -*-
"""ClickHouse 客户端工厂（全项目共用，带 ch_retry 重试）"""
import clickhouse_connect

from config import cfg
from utils.retry import ch_retry


@ch_retry
def get_ch_client(connect_timeout: int = 10, send_receive_timeout: int = 60):
    """
    获取 ClickHouse 客户端，自动重试（tenacity ch_retry）。
    connect_timeout       默认 10s（连接超时）
    send_receive_timeout  默认 60s，重型查询可传 120/300/600
    """
    return clickhouse_connect.get_client(
        host=cfg.ch_host,
        port=cfg.ch_port,
        username=cfg.ch_user,
        password=cfg.ch_password,
        connect_timeout=connect_timeout,
        send_receive_timeout=send_receive_timeout,
    )
