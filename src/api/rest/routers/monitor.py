from datetime import datetime

from fastapi import APIRouter

from src.api.rest.schemas import HealthResponse

router = APIRouter(prefix="/monitor", tags=["监控"])


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """服务健康检查"""
    from src.api.rest.dependencies import get_ch_client, get_redis_client

    ch_ok = redis_ok = kafka_ok = False

    try:
        get_ch_client().query("SELECT 1")
        ch_ok = True
    except Exception:
        pass

    try:
        get_redis_client().ping()
        redis_ok = True
    except Exception:
        pass

    try:
        import socket
        from src.common.config import cfg
        host, port = cfg.kafka_bootstrap.split(":")[0], int(cfg.kafka_bootstrap.split(":")[1])
        with socket.create_connection((host, port), timeout=3):
            kafka_ok = True
    except Exception:
        pass

    status = "healthy" if all([ch_ok, redis_ok, kafka_ok]) else "degraded"
    return HealthResponse(
        status=status,
        clickhouse=ch_ok,
        redis=redis_ok,
        kafka=kafka_ok,
        timestamp=datetime.now(),
    )


@router.get("/metrics/summary")
async def metrics_summary():
    """业务指标摘要（今日汇总 + 最新分钟窗口）"""
    from src.api.rest.dependencies import get_ch_client
    ch = get_ch_client()
    try:
        today_row = ch.query("""
            SELECT
                sum(order_cnt)   AS total_orders,
                sum(total_gmv)   AS total_gmv,
                avg(avg_price)   AS avg_price,
                max(window_end)  AS last_updated
            FROM dws.realtime_minute_stats
            WHERE window_end >= toStartOfDay(now())
        """).first_row

        latest_rows = ch.query("""
            SELECT window_end, order_cnt, total_gmv, avg_price
            FROM dws.realtime_minute_stats
            ORDER BY window_end DESC
            LIMIT 5
        """).result_rows

        return {
            "today": {
                "total_orders": int(today_row[0] or 0),
                "total_gmv":    round(float(today_row[1] or 0), 2),
                "avg_price":    round(float(today_row[2] or 0), 2),
                "last_updated": str(today_row[3]) if today_row[3] else None,
            },
            "recent_windows": [
                {
                    "window_end": str(r[0]),
                    "order_cnt":  int(r[1]),
                    "total_gmv":  round(float(r[2]), 2),
                    "avg_price":  round(float(r[3]), 2),
                }
                for r in latest_rows
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/flink/jobs")
async def flink_jobs():
    """Flink 作业状态"""
    import json
    import urllib.request
    try:
        resp = urllib.request.urlopen("http://flink-jobmanager:8081/jobs", timeout=5)
        return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}
