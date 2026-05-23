# -*- coding: utf-8 -*-
"""
告警引擎 —— 公共数据结构与导出
"""

import os
import sys
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# ── 核心事件数据结构 ─────────────────────────────────────────────

@dataclass
class AlertEvent:
    alert_id: str = ''                  # UUID，aggregator 填充
    source: str = ''                    # 'rule_engine'|'anomaly_detector'|'trend_predictor'|'lineage_impact'
    category: str = ''                  # 'DATA_QUALITY'|'SYSTEM'|'BUSINESS'|'CAPACITY'
    severity: str = 'P3'               # P1（最高）/P2/P3/P4
    title: str = ''
    detail: str = ''
    metric_name: str = ''
    current_value: float = 0.0
    threshold_value: float = 0.0
    affected_tables: list = field(default_factory=list)    # 直接受影响的表
    downstream_tables: list = field(default_factory=list)  # 血缘下游表
    context: dict = field(default_factory=dict)
    fired_at: datetime = field(default_factory=datetime.now)
    fingerprint: str = ''              # dedup key，hash(source+metric_name+category)

    def compute_fingerprint(self) -> str:
        """计算并缓存告警指纹（去重键）"""
        raw = f"{self.source}:{self.metric_name}:{self.category}"
        self.fingerprint = hashlib.md5(raw.encode()).hexdigest()[:16]
        return self.fingerprint


# ── 公共导出 ─────────────────────────────────────────────────────

__all__ = [
    'AlertEvent',
]
