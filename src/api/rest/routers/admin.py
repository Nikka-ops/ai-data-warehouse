from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/admin", tags=["管理"])

@router.post("/schema/refresh")
async def refresh_schema():
    """刷新 NL2SQL Schema 缓存"""
    ...

@router.post("/rag/index")
async def rebuild_rag_index():
    """重建 RAG 知识库索引"""
    ...

@router.get("/system/info")
async def system_info():
    """系统信息"""
    import platform
    return {"python": platform.python_version(), "platform": platform.system()}
