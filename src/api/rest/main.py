from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.rest.routers import query

app = FastAPI(
    title="AI Data Warehouse API",
    description="Kappa 架构实时数仓 AI 查询服务",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

app.include_router(query.router, prefix="/api/v1")

@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
