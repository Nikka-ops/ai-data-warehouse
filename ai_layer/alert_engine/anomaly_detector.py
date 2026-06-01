# -*- coding: utf-8 -*-
"""
异常检测器 —— 2σ 统计检测 + PSI 特征漂移检测
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import numpy as np

from utils.logger import get_logger
from ai_layer.alert_engine import AlertEvent

log = get_logger('alert_engine.anomaly_detector')

# ── 2σ 异常检测 ──────────────────────────────────────────────────

_SIGMA_METRICS = [
    {
        "metric_name": "order_cnt_anomaly",
        "column": "order_cnt",
        "title": "订单量异常下降（2σ）",
        "category": "DATA_QUALITY",
        "severity": "P2",
    },
    {
        "metric_name": "gmv_anomaly",
        "column": "total_gmv",
        "title": "GMV 异常下降（2σ）",
        "category": "BUSINESS",
        "severity": "P2",
    },
]

_BASELINE_MINUTES = 60   # 基线窗口（分钟）
_CURRENT_MINUTES  = 5    # 当前窗口（分钟）
_MIN_BASELINE_PTS = 10   # 基线数据量最小要求


def _detect_sigma(ch) -> list:
    """
    对 order_cnt / total_gmv 做 2σ 下降检测。
    基线：过去60分钟，当前：最近5分钟均值。
    若 current < baseline_mean - 2*std 且基线点数>=10，产出 P2 告警。
    """
    alerts = []

    for cfg_item in _SIGMA_METRICS:
        col = cfg_item["column"]
        metric_name = cfg_item["metric_name"]

        # 查基线数据（逐分钟粒度）
        baseline_vals_sql = (
            f"SELECT {col}"
            f" FROM dws.realtime_minute_stats"
            f" WHERE window_start >= now() - INTERVAL {_BASELINE_MINUTES} MINUTE"
            f"   AND window_start <  now() - INTERVAL {_CURRENT_MINUTES} MINUTE"
            f" ORDER BY window_start"
        )
        current_sql = (
            f"SELECT avg({col})"
            f" FROM dws.realtime_minute_stats"
            f" WHERE window_start >= now() - INTERVAL {_CURRENT_MINUTES} MINUTE"
        )

        try:
            baseline_rows = ch.query(baseline_vals_sql).result_rows
            current_rows  = ch.query(current_sql).result_rows
        except Exception as exc:
            log.warning('[2σ/%s] 查询失败（跳过）：%s', metric_name, exc)
            continue

        if not baseline_rows or len(baseline_rows) < _MIN_BASELINE_PTS:
            log.debug('[2σ/%s] 基线数据不足（%d 条），跳过', metric_name, len(baseline_rows))
            continue

        if not current_rows or current_rows[0][0] is None:
            log.debug('[2σ/%s] 当前值为空，跳过', metric_name)
            continue

        baseline_arr = np.array(
            [float(r[0]) for r in baseline_rows if r[0] is not None],
            dtype=float,
        )
        if len(baseline_arr) < _MIN_BASELINE_PTS:
            continue

        baseline_mean = float(np.mean(baseline_arr))
        baseline_std  = float(np.std(baseline_arr, ddof=1))
        current_val   = float(current_rows[0][0])

        lower_bound = baseline_mean - 2.0 * baseline_std

        log.debug(
            '[2σ/%s] current=%.2f  mean=%.2f  std=%.2f  lower=%.2f',
            metric_name, current_val, baseline_mean, baseline_std, lower_bound,
        )

        if current_val < lower_bound:
            detail = (
                f"{col} 当前均值 {current_val:.2f}，"
                f"基线均值 {baseline_mean:.2f}±{baseline_std:.2f}，"
                f"低于 2σ 下界 {lower_bound:.2f}"
            )
            log.info('[2σ/%s] 告警触发：%s', metric_name, detail)
            event = AlertEvent(
                source='anomaly_detector',
                category=cfg_item["category"],
                severity=cfg_item["severity"],
                title=cfg_item["title"],
                detail=detail,
                metric_name=metric_name,
                current_value=current_val,
                threshold_value=lower_bound,
                affected_tables=["dws.realtime_minute_stats"],
                context={
                    "baseline_mean": baseline_mean,
                    "baseline_std": baseline_std,
                    "lower_bound": lower_bound,
                    "baseline_points": len(baseline_arr),
                },
            )
            event.compute_fingerprint()
            alerts.append(event)

    return alerts


# ── PSI 漂移检测 ─────────────────────────────────────────────────

_PSI_SQL = (
    "SELECT feature_name, psi_score"
    " FROM feature_store.drift_stats"
    " WHERE drift_detected = 1"
    " ORDER BY psi_score DESC"
)


def _detect_psi(ch) -> list:
    """
    查询 feature_store.drift_stats 中已标记漂移的特征，
    每个特征产出一个 P3 告警。
    """
    alerts: list = []
    try:
        rows = ch.query(_PSI_SQL).result_rows
    except Exception as exc:
        log.warning('[PSI] 查询失败（跳过）：%s', exc)
        return alerts

    for row in rows:
        if not row or len(row) < 2:
            continue
        feature_name = str(row[0])
        psi_score    = float(row[1]) if row[1] is not None else 0.0

        title  = f"特征漂移：{feature_name}（PSI={psi_score:.4f}）"
        detail = (
            f"特征 {feature_name} PSI={psi_score:.4f}，"
            f"已超过漂移检测阈值，请检查数据分布变化。"
        )
        log.info('[PSI] 漂移告警：%s', title)

        event = AlertEvent(
            source='anomaly_detector',
            category='DATA_QUALITY',
            severity='P3',
            title=title,
            detail=detail,
            metric_name=f'psi_{feature_name}',
            current_value=psi_score,
            threshold_value=0.0,
            affected_tables=["feature_store.feature_values"],
            context={"feature_name": feature_name, "psi_score": psi_score},
        )
        event.compute_fingerprint()
        alerts.append(event)

    return alerts


# ── 对外接口 ─────────────────────────────────────────────────────

def run(ch) -> list:
    """
    执行 2σ + PSI 检测，返回触发的 AlertEvent 列表。
    任一子检测失败只记 warning，不中断整体流程。
    """
    alerts = []

    try:
        sigma_alerts = _detect_sigma(ch)
        alerts.extend(sigma_alerts)
    except Exception as exc:
        log.warning('[anomaly_detector] 2σ 检测异常（跳过）：%s', exc)

    try:
        psi_alerts = _detect_psi(ch)
        alerts.extend(psi_alerts)
    except Exception as exc:
        log.warning('[anomaly_detector] PSI 检测异常（跳过）：%s', exc)

    log.info('异常检测完成：触发 %d 条告警', len(alerts))
    return alerts
