# -*- coding: utf-8 -*-
"""数据新鲜度追踪：监控各表的最新数据时间"""
from datetime import datetime, timedelta


class FreshnessTracker:
    FRESHNESS_THRESHOLDS = {
        "dws.realtime_minute_stats": timedelta(minutes=2),
        "dws.kappa_hourly_agg":      timedelta(hours=2),
        "dws.kappa_serving_unified": timedelta(hours=1),
    }

    def __init__(self, ch):  # clickhouse_connect.Client
        self.ch = ch

    def check(self, table: str) -> dict:
        """检查表的数据新鲜度"""
        try:
            result = self.ch.query(f"SELECT max(event_time) FROM {table}").result_rows
            last_time = result[0][0] if result else None
            threshold = self.FRESHNESS_THRESHOLDS.get(table, timedelta(hours=24))
            stale = last_time is None or (datetime.now() - last_time) > threshold
            return {"table": table, "last_update": last_time, "stale": stale}
        except Exception as e:
            return {"table": table, "error": str(e), "stale": True}

    def check_all(self) -> list[dict]:
        return [self.check(t) for t in self.FRESHNESS_THRESHOLDS]
