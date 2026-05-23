from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
import os

class JWTAuthMiddleware(BaseHTTPMiddleware):
    ENABLED = os.getenv("ENABLE_AUTH", "false").lower() == "true"
    PUBLIC_PATHS = {"/health", "/ready", "/docs", "/redoc", "/openapi.json"}

    async def dispatch(self, request: Request, call_next):
        if not self.ENABLED or request.url.path in self.PUBLIC_PATHS:
            return await call_next(request)
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if not token:
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "未提供认证 Token"}, status_code=401)
        return await call_next(request)
