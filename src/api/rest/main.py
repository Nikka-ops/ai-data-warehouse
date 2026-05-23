from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.rest.routers import query, alert, lineage, monitor, admin
from src.api.middleware.logging import RequestLoggingMiddleware

app = FastAPI(
    title="AI Data Warehouse API",
    description="Kappa 架构实时数仓 AI 查询服务",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])
app.add_middleware(RequestLoggingMiddleware)

app.include_router(query.router,   prefix="/api/v1")
app.include_router(alert.router,   prefix="/api/v1")
app.include_router(lineage.router, prefix="/api/v1")
app.include_router(monitor.router, prefix="/api/v1")
app.include_router(admin.router,   prefix="/api/v1")

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/ready")
async def ready():
    return {"status": "ready"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
