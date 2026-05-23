# -*- coding: utf-8 -*-
"""
Confluent Schema Registry 客户端
支持 Avro schema 的注册、查询（按 subject 和按 ID）
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error

from src.common.utils import get_logger

log = get_logger("ingestion.schema.registry")


class SchemaRegistryClient:
    """与 Confluent Schema Registry REST API 交互"""

    def __init__(self, url: str = "http://schema-registry:8081") -> None:
        self.url = url.rstrip("/")  # 去除末尾斜杠，避免拼接双斜杠

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        """通用 HTTP 请求封装，返回解析后的 JSON"""
        full_url = f"{self.url}{path}"
        data     = json.dumps(body).encode() if body else None
        req      = urllib.request.Request(
            full_url,
            data=data,
            method=method,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            log.error("Schema Registry 请求失败 %s %s：%s", method, path, exc)
            raise

    def register(self, subject: str, schema_str: str) -> int:
        """
        注册 Avro schema 到指定 subject
        返回 schema_id（Registry 自动去重，相同 schema 返回已有 ID）
        """
        log.info("注册 schema subject=%s", subject)
        resp = self._request(
            "POST",
            f"/subjects/{subject}/versions",
            body={"schema": schema_str},
        )
        return resp["id"]  # 返回 schema_id

    def get_latest(self, subject: str) -> dict:
        """获取指定 subject 的最新版本 schema 信息"""
        log.debug("获取最新 schema subject=%s", subject)
        return self._request("GET", f"/subjects/{subject}/versions/latest")

    def get_by_id(self, schema_id: int) -> str:
        """按 schema_id 查询 schema 字符串"""
        log.debug("按 ID 获取 schema id=%d", schema_id)
        resp = self._request("GET", f"/schemas/ids/{schema_id}")
        return resp["schema"]  # 返回 schema JSON 字符串
