# -*- coding: utf-8 -*-
"""
Debezium PostgreSQL CDC 配置生成器
生成 Kafka Connect Debezium PostgreSQL Connector 配置，并支持向 Connect REST API 注册
需在 PostgreSQL 端提前开启逻辑复制：wal_level = logical
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from src.common.utils import get_logger

log = get_logger("ingestion.cdc.postgres")


class PostgresCDCConfig:
    """生成 Debezium PostgreSQL Connector 配置 JSON，不负责运行时 CDC 逻辑"""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        server_name: str,
        schema_include: str = "public",
        plugin_name: str = "pgoutput",
    ) -> None:
        self.host           = host
        self.port           = port
        self.user           = user
        self.password       = password      # 密码从外部传入，不在代码中硬编码
        self.database       = database
        self.server_name    = server_name   # Kafka topic 前缀，需全局唯一
        self.schema_include = schema_include
        self.plugin_name    = plugin_name   # pgoutput（PG 10+）或 wal2json / decoderbufs

    def to_connector_config(self) -> dict:
        """返回 Debezium PostgreSQL Connector 的完整配置字典"""
        return {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": self.host,
            "database.port": str(self.port),
            "database.user": self.user,
            "database.password": self.password,
            "database.dbname": self.database,
            "database.server.name": self.server_name,
            "schema.include.list": self.schema_include,   # 捕获指定 schema 的变更
            "plugin.name": self.plugin_name,              # 逻辑解码插件
            "slot.name": f"debezium_{self.server_name}",  # 复制槽名（需唯一）
            "publication.name": f"dbz_{self.server_name}",  # publication 名
            "snapshot.mode": "initial",                   # 首次全量快照
            "include.schema.changes": "false",            # 不捕获 DDL
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
        log.info("注册 PostgreSQL CDC connector：%s → %s", self.server_name, connect_url)
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
