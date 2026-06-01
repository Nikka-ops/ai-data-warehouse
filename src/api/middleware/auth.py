from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
import os

class JWTAuthMiddleware(BaseHTTPMiddleware):
    ENABLED = os.getenv("ENABLE_AUTH", "false").lower() == "true"
    PUBLIC_PATHS = {"/health", "/ready", "/docs", "/redoc", "/openapi.json"}
    _SECRET = os.getenv("JWT_SECRET", "")

    async def dispatch(self, request: Request, call_next):
        if not self.ENABLED or request.url.path in self.PUBLIC_PATHS:
            return await call_next(request)
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if not token:
            return JSONResponse({"detail": "未提供认证 Token"}, status_code=401)
        if not self._SECRET:
            return JSONResponse({"detail": "服务器未配置 JWT_SECRET"}, status_code=500)
        try:
            import jwt
            jwt.decode(token, self._SECRET, algorithms=["HS256"])
        except jwt.ExpiredSignatureError:
            return JSONResponse({"detail": "Token 已过期"}, status_code=401)
        except jwt.InvalidTokenError:
            return JSONResponse({"detail": "Token 无效"}, status_code=401)
        return await call_next(request)
