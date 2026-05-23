import os
import sys

from fastapi import APIRouter

router = APIRouter(prefix="/admin", tags=["管理"])

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))


@router.post("/schema/refresh")
async def refresh_schema():
    """刷新 NL2SQL Schema 缓存"""
    from ai_layer.nl2sql import invalidate_schema_cache
    invalidate_schema_cache()
    return {"status": "ok", "message": "Schema 缓存已清除，下次查询时自动重建"}


@router.post("/rag/index")
async def rebuild_rag_index():
    """重建 RAG 知识库索引"""
    from ai_layer.rag_engine import build_knowledge_base
    col = build_knowledge_base(force_rebuild=True)
    chunk_count = col.count() if col is not None else 0
    return {"status": "ok", "chunks": chunk_count, "message": f"知识库重建完成，共 {chunk_count} 个文本块"}


@router.get("/system/info")
async def system_info():
    """系统信息"""
    import platform
    return {"python": platform.python_version(), "platform": platform.system()}
