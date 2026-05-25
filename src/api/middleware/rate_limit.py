import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


class RateLimitMiddleware(BaseHTTPMiddleware):
    """固定窗口限流：每个 IP 每窗口期最多 max_requests 次。

    优先使用 Redis 共享计数器（多 Worker 部署下全局生效），
    Redis 不可用时降级为进程内计数（单 Worker 有效）。
    """

    def __init__(self, app, max_requests: int = 60, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._redis = None
        self._local: dict[str, list] = {}  # 降级用的进程内计数

    def _get_redis(self):
        if self._redis is not None:
            return self._redis
        try:
            import redis as redis_lib
            from src.common.config import cfg
            r = redis_lib.Redis(host=cfg.redis_host, port=cfg.redis_port,
                                socket_connect_timeout=1, decode_responses=True)
            r.ping()
            self._redis = r
        except Exception:
            self._redis = None
        return self._redis

    async def dispatch(self, request, call_next):
        ip = request.client.host if request.client else "unknown"
        now = time.time()

        r = self._get_redis()
        if r is not None:
            # Redis 固定窗口：INCR + EXPIRE 原子操作，所有 Worker 共享计数
            slot = int(now // self.window_seconds)
            key = f"ratelimit:{ip}:{slot}"
            try:
                count = r.incr(key)
                if count == 1:
                    r.expire(key, self.window_seconds + 1)
                if count > self.max_requests:
                    return JSONResponse({"detail": "请求过于频繁，请稍后重试"}, status_code=429)
            except Exception:
                pass  # Redis 故障时放行，避免影响可用性
        else:
            # 进程内降级：滑动窗口（仅单 Worker 有效）
            window = [t for t in self._local.get(ip, []) if now - t < self.window_seconds]
            if len(window) >= self.max_requests:
                return JSONResponse({"detail": "请求过于频繁，请稍后重试"}, status_code=429)
            window.append(now)
            self._local[ip] = window

        return await call_next(request)
