# -*- coding: utf-8 -*-
"""
Debezium MySQL CDC 配置生成器
生成 Kafka Connect Debezium MySQL Connector 的配置 JSON，并支持向 Connect REST API 注册
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from src.common.utils import get_logger

log = get_logger("ingestion.cdc.mysql")


class MySQLCDCConfig:
    """生成 Debezium MySQL Connector 配置 JSON，不负责运行时 CDC 逻辑"""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        server_name: str,
    ) -> None:
        self.host        = host
        self.port        = port
        self.user        = user
        self.password    = password     # 密码从外部传入，不在代码中硬编码
        self.database    = database
        self.server_name = server_name  # Kafka topic 前缀，需全局唯一

    def to_connector_config(self) -> dict:
        """返回 Debezium MySQL Connector 的完整配置字典"""
        return {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": self.host,
            "database.port": str(self.port),
            "database.user": self.user,
            "database.password": self.password,
            "database.server.name": self.server_name,
            "database.include.list": self.database,
            "include.schema.changes": "false",          # 不捕获 DDL 变更
            "snapshot.mode": "initial",                 # 首次启动全量快照
            "database.history.kafka.topic": f"dbhistory.{self.server_name}",
            "transforms": "route",
            "transforms.route.type": (
                "org.apache.kafka.connect.transforms.ReplaceField$Value"
            ),
        }

    def register(self, connect_url: str = "http://localhost:8083") -> None:
        """向 Kafka Connect REST API 注册 connector"""
        payload = {
            "name": self.server_name,
            "config": self.to_connector_config(),
        }
        data = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            f"{connect_url}/connectors",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        log.info("注册 MySQL CDC connector：%s → %s", self.server_name, connect_url)
        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read().decode()
                log.info("注册成功：%s", body[:200])
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                log.info("Connector %s 已存在（409 Conflict），跳过注册", self.server_name)
            else:
                body = exc.read().decode(errors="replace")
                raise RuntimeError(
                    f"注册 connector {self.server_name} 失败 (HTTP {exc.code})：{body[:200]}"
                ) from exc
