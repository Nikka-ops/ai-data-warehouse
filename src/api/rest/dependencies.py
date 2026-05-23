from functools import lru_cache
import clickhouse_connect

@lru_cache(maxsize=1)
def get_ch_client():
    from src.common.config import cfg
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password)

@lru_cache(maxsize=1)
def get_redis_client():
    import redis
    from src.common.config import cfg
    return redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=True)
