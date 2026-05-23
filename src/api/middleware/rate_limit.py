import time
from collections import defaultdict
from starlette.middleware.base import BaseHTTPMiddleware

class RateLimitMiddleware(BaseHTTPMiddleware):
    """令牌桶限流：每个 IP 每分钟最多 60 次"""
    def __init__(self, app, max_requests: int = 60, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._buckets: dict = defaultdict(list)

    async def dispatch(self, request, call_next):
        ip = request.client.host if request.client else "unknown"
        now = time.time()
        window = self._buckets[ip]
        # 清理过期记录
        self._buckets[ip] = [t for t in window if now - t < self.window_seconds]
        if len(self._buckets[ip]) >= self.max_requests:
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "请求过于频繁，请稍后重试"}, status_code=429)
        self._buckets[ip].append(now)
        return await call_next(request)
