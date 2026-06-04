from functools import lru_cache
from src.storage.clickhouse.client import get_client as _get_ch_client

@lru_cache(maxsize=1)
def get_ch_client():
    return _get_ch_client()

@lru_cache(maxsize=1)
def get_redis_client():
    import redis
    from config import cfg
    return redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=True)
