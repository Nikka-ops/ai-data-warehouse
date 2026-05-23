# -*- coding: utf-8 -*-
"""
Redis 连接配置，提供同步和异步客户端工厂，以及全局连接池
"""

from __future__ import annotations

try:
    import redis
    from redis import ConnectionPool
    _REDIS_AVAILABLE = True
except ImportError:
    redis = None  # type: ignore[assignment]
    _REDIS_AVAILABLE = False  # 缺少依赖时不崩溃

try:
    import redis.asyncio as aioredis
    _AIOREDIS_AVAILABLE = True
except ImportError:
    aioredis = None  # type: ignore[assignment]
    _AIOREDIS_AVAILABLE = False  # 异步客户端可选依赖

from src.common.config import cfg
from src.common.utils import get_logger

log = get_logger("storage.redis")

# 全局同步连接池（最大 50 个连接）
_REDIS_POOL = None


def _get_pool():
    """懒加载全局连接池"""
    global _REDIS_POOL
    if _REDIS_POOL is None:
        if not _REDIS_AVAILABLE:
            raise ImportError("redis 未安装，请执行 pip install redis")
        _REDIS_POOL = ConnectionPool(
            host=cfg.redis_host,
            port=cfg.redis_port,
            max_connections=50,         # 连接池上限
            decode_responses=True,      # 自动解码为 str
        )
        log.info("Redis 连接池初始化 %s:%s", cfg.redis_host, cfg.redis_port)
    return _REDIS_POOL


def get_redis_client() -> "redis.Redis":
    """获取同步 Redis 客户端，复用全局连接池"""
    pool = _get_pool()
    return redis.Redis(connection_pool=pool)


def get_async_redis_client() -> "aioredis.Redis":
    """获取异步 Redis 客户端，用于 asyncio 场景"""
    if not _AIOREDIS_AVAILABLE:
        raise ImportError("redis[asyncio] 未安装，请执行 pip install redis")
    return aioredis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        decode_responses=True,  # 自动解码为 str
    )


# 暴露连接池引用（供外部监控或 flush 使用）
REDIS_POOL = _REDIS_POOL
