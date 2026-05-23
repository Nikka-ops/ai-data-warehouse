# -*- coding: utf-8 -*-
"""
Redis 在线特征缓存，提供单条和批量 get/set 操作
key 格式：feat:{entity_type}:{entity_id}
"""

from __future__ import annotations

import json
from typing import Any

from src.common.utils import get_logger
from src.storage.redis.redis_config import get_redis_client

log = get_logger("storage.redis.feature_cache")

_KEY_PREFIX = "feat"  # 特征 key 前缀


def _make_key(entity_type: str, entity_id: str) -> str:
    """构造 Redis key：feat:{entity_type}:{entity_id}"""
    return f"{_KEY_PREFIX}:{entity_type}:{entity_id}"


class FeatureCache:
    """Redis 特征缓存，支持单条和批量读写"""

    def __init__(self) -> None:
        self._client = None  # 懒加载，避免启动时连接失败阻塞进程

    @property
    def client(self):
        """懒加载 Redis 连接"""
        if self._client is None:
            self._client = get_redis_client()
        return self._client

    # ── 单条操作 ────────────────────────────────────────────────────

    def get(self, entity_type: str, entity_id: str) -> dict | None:
        """读取单个实体特征；缓存未命中时返回 None"""
        key = _make_key(entity_type, entity_id)
        raw = self.client.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.warning("特征缓存 JSON 解析失败，key=%s", key)
            return None

    def set(
        self,
        entity_type: str,
        entity_id: str,
        features: dict,
        ttl: int = 3600,
    ) -> None:
        """写入单个实体特征，ttl 秒后自动过期"""
        key = _make_key(entity_type, entity_id)
        self.client.setex(key, ttl, json.dumps(features, ensure_ascii=False))
        log.debug("特征写入 key=%s ttl=%ds", key, ttl)

    def delete(self, entity_type: str, entity_id: str) -> None:
        """删除单个实体特征缓存"""
        key = _make_key(entity_type, entity_id)
        self.client.delete(key)
        log.debug("特征删除 key=%s", key)

    # ── 批量操作 ────────────────────────────────────────────────────

    def mget(
        self,
        entity_type: str,
        entity_ids: list[str],
    ) -> list[dict | None]:
        """批量读取多个实体特征；未命中的位置返回 None"""
        if not entity_ids:
            return []
        keys = [_make_key(entity_type, eid) for eid in entity_ids]
        raws = self.client.mget(keys)  # 单次网络往返
        results: list[dict | None] = []
        for key, raw in zip(keys, raws):
            if raw is None:
                results.append(None)
            else:
                try:
                    results.append(json.loads(raw))
                except json.JSONDecodeError:
                    log.warning("批量特征 JSON 解析失败，key=%s", key)
                    results.append(None)
        return results

    def mset(
        self,
        entity_type: str,
        items: dict[str, dict],
        ttl: int = 3600,
    ) -> None:
        """批量写入多个实体特征（pipeline 减少往返次数）"""
        if not items:
            return
        pipe = self.client.pipeline(transaction=False)
        for entity_id, features in items.items():
            key = _make_key(entity_type, entity_id)
            pipe.setex(key, ttl, json.dumps(features, ensure_ascii=False))
        pipe.execute()
        log.debug("批量特征写入 entity_type=%s count=%d", entity_type, len(items))
