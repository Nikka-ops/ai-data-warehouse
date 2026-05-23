from fastapi import APIRouter

router = APIRouter(prefix="/monitor", tags=["监控"])

@router.get("/health")
async def health_check():
    """服务健康检查"""
    ...  # 检查 ClickHouse/Redis/Kafka 连通性

@router.get("/metrics/summary")
async def metrics_summary():
    """业务指标摘要"""
    ...

@router.get("/flink/jobs")
async def flink_jobs():
    """Flink 作业状态"""
    import urllib.request
    import json
    try:
        resp = urllib.request.urlopen("http://flink-jobmanager:8081/jobs", timeout=5)
        return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}
