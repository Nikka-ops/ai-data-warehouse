import time
from starlette.middleware.base import BaseHTTPMiddleware
from src.common.utils import get_logger

log = get_logger('api.request')

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start = time.time()
        response = await call_next(request)
        elapsed_ms = (time.time() - start) * 1000
        log.info("%s %s %d %.1fms", request.method, request.url.path,
                 response.status_code, elapsed_ms)
        return response
