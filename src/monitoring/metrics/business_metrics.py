# -*- coding: utf-8 -*-
"""定期采集业务指标（GMV、订单量、特征新鲜度）"""
import time
from src.common.utils import get_logger

log = get_logger('monitoring.business_metrics')

class BusinessMetricsCollector:
    def __init__(self, ch):
        self.ch = ch

    def collect_gmv(self) -> float:
        """当日 GMV"""
        try:
            rows = self.ch.query("SELECT sum(total_gmv) FROM dws.realtime_minute_stats WHERE window_start >= today()").result_rows
            return float(rows[0][0]) if rows and rows[0][0] else 0.0
        except Exception:
            return 0.0

    def collect_order_count(self) -> int:
        """当日订单量"""
        try:
            rows = self.ch.query("SELECT sum(order_cnt) FROM dws.realtime_minute_stats WHERE window_start >= today()").result_rows
            return int(rows[0][0]) if rows and rows[0][0] else 0
        except Exception:
            return 0

    def collect_all(self) -> dict:
        return {
            "gmv": self.collect_gmv(),
            "order_count": self.collect_order_count(),
            "collected_at": time.time(),
        }
