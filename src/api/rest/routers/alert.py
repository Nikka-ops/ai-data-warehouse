from fastapi import APIRouter
from src.api.rest.schemas import AlertRequest, AlertResponse

router = APIRouter(prefix="/alert", tags=["告警"])

@router.get("/list")
async def list_alerts(limit: int = 20):
    """查询最新告警列表"""
    ...

@router.post("/diagnose", response_model=AlertResponse)
async def diagnose_alert(req: AlertRequest):
    """对告警进行 AI 诊断"""
    ...

@router.get("/stats")
async def alert_stats():
    """告警统计（按严重程度/来源分布）"""
    ...
