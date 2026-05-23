import os
import sys
import uuid
from datetime import datetime

from fastapi import APIRouter

from src.api.rest.schemas import AlertRequest, AlertResponse

router = APIRouter(prefix="/alert", tags=["告警"])

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))


@router.get("/list")
async def list_alerts(limit: int = 20):
    """查询最新告警列表"""
    from src.api.rest.dependencies import get_ch_client
    ch = get_ch_client()
    try:
        rows = ch.query(f"""
            SELECT alert_time, alert_type, severity, field_name, detail,
                   metric_value, threshold_value
            FROM stream.ai_quality_alerts
            ORDER BY alert_time DESC
            LIMIT {limit}
        """).result_rows
        return {
            "alerts": [
                {
                    "alert_time":      str(r[0]),
                    "alert_type":      r[1],
                    "severity":        r[2],
                    "field_name":      r[3],
                    "detail":          r[4],
                    "metric_value":    float(r[5]) if r[5] is not None else None,
                    "threshold_value": float(r[6]) if r[6] is not None else None,
                }
                for r in rows
            ],
            "count": len(rows),
        }
    except Exception as e:
        return {"alerts": [], "count": 0, "error": str(e)}


@router.post("/diagnose", response_model=AlertResponse)
async def diagnose_alert(req: AlertRequest):
    """对告警进行 AI 诊断（调用 LangGraph Alert Orchestrator）"""
    from ai_layer.alert_engine.orchestrator import AlertOrchestrator
    from src.api.rest.dependencies import get_ch_client

    class _AlertObj:
        def __init__(self, r: AlertRequest):
            self.alert_id = str(uuid.uuid4())
            self.source = r.source
            self.severity = r.severity
            self.title = r.title
            self.detail = r.detail
            self.metric_name = r.metric_name
            self.current_value = r.current_value
            self.affected_tables = []
            self.downstream_tables = []
            self.fired_at = datetime.now()

    ch = get_ch_client()
    alert = _AlertObj(req)
    result = AlertOrchestrator(ch).handle(alert)

    plan_raw = result.get("plan", "")
    actions: list[str] = []
    try:
        import json
        plan = json.loads(plan_raw) if isinstance(plan_raw, str) and plan_raw.strip() else {}
        actions = plan.get("steps", []) if isinstance(plan, dict) else []
    except Exception:
        pass

    return AlertResponse(
        alert_id=alert.alert_id,
        diagnosis=result.get("final_report", ""),
        actions=actions,
        escalated=result.get("escalated", False),
    )


@router.get("/stats")
async def alert_stats():
    """告警统计（过去 24h，按严重度 / 类型分布）"""
    from src.api.rest.dependencies import get_ch_client
    ch = get_ch_client()
    try:
        by_severity = ch.query("""
            SELECT severity, count() AS cnt
            FROM stream.ai_quality_alerts
            WHERE alert_time >= now() - INTERVAL 24 HOUR
            GROUP BY severity
            ORDER BY cnt DESC
        """).result_rows

        by_type = ch.query("""
            SELECT alert_type, count() AS cnt
            FROM stream.ai_quality_alerts
            WHERE alert_time >= now() - INTERVAL 24 HOUR
            GROUP BY alert_type
            ORDER BY cnt DESC
        """).result_rows

        total = sum(r[1] for r in by_severity)
        return {
            "window": "24h",
            "total": total,
            "by_severity": {r[0]: r[1] for r in by_severity},
            "by_type":     {r[0]: r[1] for r in by_type},
        }
    except Exception as e:
        return {"error": str(e)}
