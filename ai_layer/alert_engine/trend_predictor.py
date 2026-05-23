# -*- coding: utf-8 -*-
"""
趋势预测器 —— 容量/延迟趋势预警
使用 numpy 线性回归外推预测未来值，不引入 statsmodels 等新依赖。
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import numpy as np

from utils.logger import get_logger
from ai_layer.alert_engine import AlertEvent

log = get_logger('alert_engine.trend_predictor')

# ── 模块级状态（磁盘容量历史采样点）────────────────────────────────
# 每次 run() 会追加一个 (timestamp_seconds, bytes_on_disk) 记录，最多保留 20 个
_disk_history: list[tuple[float, float]] = []
_MAX_DISK_HISTORY = 20

# Kafka Lag 预测参数
_LAG_SAMPLE_LIMIT = 20           # 最多取最近 N 个采样点
_LAG_PREDICT_MINUTES = 30        # 预测未来 N 分钟后的 lag
_LAG_PREDICT_THRESHOLD = 100000  # 预测值超过此阈值触发 P2

# 磁盘增长率阈值：1 GB/小时 → bytes/second
_DISK_SLOPE_THRESHOLD = 1 * 1024 ** 3 / 3600   # ≈ 292984 bytes/s
_DISK_TREND_MIN_POINTS = 5                       # 至少需要5个点才做趋势判断


# ── 线性回归辅助函数 ─────────────────────────────────────────────

def _linear_extrapolate(xs: np.ndarray, ys: np.ndarray, x_future: float) -> tuple[float, float]:
    """
    给定 (xs, ys) 数组，用 numpy 最小二乘拟合直线，
    返回 (slope, predicted_y_at_x_future)。
    """
    # 归一化 x，避免数值精度问题
    x0 = xs[0]
    xs_norm = xs - x0
    x_future_norm = x_future - x0

    # y = a*x + b，numpy polyfit degree=1
    coeffs = np.polyfit(xs_norm, ys, 1)
    slope = float(coeffs[0])
    predicted = float(np.polyval(coeffs, x_future_norm))
    return slope, predicted


# ── Kafka Lag 趋势检测 ───────────────────────────────────────────

_LAG_SQL = (
    f"SELECT toUnixTimestamp(check_time), lag"
    f" FROM stream.kappa_consumer_lag"
    f" ORDER BY check_time DESC"
    f" LIMIT {_LAG_SAMPLE_LIMIT}"
)


def _predict_kafka_lag(ch) -> AlertEvent | None:
    """
    取最近 N 个 Kafka Lag 采样点，线性外推30分钟后的 lag。
    若预测值 > 100000，返回 P2 CAPACITY 告警。
    """
    try:
        rows = ch.query(_LAG_SQL).result_rows
    except Exception as exc:
        log.warning('[trend/kafka_lag] 查询失败（跳过）：%s', exc)
        return None

    if not rows or len(rows) < 3:
        log.debug('[trend/kafka_lag] 采样点不足（%d），跳过', len(rows) if rows else 0)
        return None

    # 查询结果是 DESC 顺序，反转为时间升序
    rows_asc = list(reversed(rows))
    try:
        xs = np.array([float(r[0]) for r in rows_asc], dtype=float)
        ys = np.array([float(r[1]) for r in rows_asc if r[1] is not None], dtype=float)
    except (TypeError, ValueError) as exc:
        log.warning('[trend/kafka_lag] 数据转换失败（跳过）：%s', exc)
        return None

    if len(xs) != len(ys) or len(xs) < 3:
        log.debug('[trend/kafka_lag] 有效采样点不足，跳过')
        return None

    x_future = xs[-1] + _LAG_PREDICT_MINUTES * 60  # 30分钟后的 Unix 时间戳
    try:
        slope, predicted = _linear_extrapolate(xs, ys, x_future)
    except Exception as exc:
        log.warning('[trend/kafka_lag] 线性回归失败（跳过）：%s', exc)
        return None

    current_lag = ys[-1]
    log.debug(
        '[trend/kafka_lag] 当前 lag=%.0f  slope=%.2f/s  预测30min后=%.0f',
        current_lag, slope, predicted,
    )

    if predicted <= _LAG_PREDICT_THRESHOLD:
        return None

    title = f"Kafka Lag 趋势告警：预测30分钟后达 {predicted:.0f}"
    detail = (
        f"当前最大 lag={current_lag:.0f}，"
        f"线性趋势斜率={slope:.2f}/s，"
        f"预计30分钟后达 {predicted:.0f}（阈值 {_LAG_PREDICT_THRESHOLD}）"
    )
    log.info('[trend/kafka_lag] 趋势告警触发：%s', title)

    event = AlertEvent(
        source='trend_predictor',
        category='CAPACITY',
        severity='P2',
        title=title,
        detail=detail,
        metric_name='kafka_lag_trend',
        current_value=current_lag,
        threshold_value=float(_LAG_PREDICT_THRESHOLD),
        affected_tables=['stream.kappa_consumer_lag'],
        context={
            'slope_per_second': slope,
            'predicted_lag': predicted,
            'predict_minutes': _LAG_PREDICT_MINUTES,
            'sample_points': len(xs),
        },
    )
    event.compute_fingerprint()
    return event


# ── 磁盘容量趋势检测 ─────────────────────────────────────────────

_DISK_SQL = "SELECT sum(bytes_on_disk) FROM system.parts WHERE active"


def _predict_disk_growth(ch) -> AlertEvent | None:
    """
    每次调用查询当前磁盘用量，追加到历史列表。
    若已有 >= _DISK_TREND_MIN_POINTS 个点，用近5个点做线性趋势判断。
    若斜率 > 1 GB/小时，返回 P3 CAPACITY 告警。
    """
    global _disk_history

    try:
        rows = ch.query(_DISK_SQL).result_rows
    except Exception as exc:
        log.warning('[trend/disk] 查询失败（跳过）：%s', exc)
        return None

    if not rows or rows[0][0] is None:
        log.debug('[trend/disk] 磁盘查询返回空，跳过')
        return None

    current_bytes = float(rows[0][0])
    now_ts = time.time()

    _disk_history.append((now_ts, current_bytes))
    # 保留最多 _MAX_DISK_HISTORY 个点
    if len(_disk_history) > _MAX_DISK_HISTORY:
        _disk_history = _disk_history[-_MAX_DISK_HISTORY:]

    if len(_disk_history) < _DISK_TREND_MIN_POINTS:
        log.debug(
            '[trend/disk] 历史采样点不足（%d/%d），跳过趋势判断',
            len(_disk_history), _DISK_TREND_MIN_POINTS,
        )
        return None

    # 取近5个点
    recent = _disk_history[-_DISK_TREND_MIN_POINTS:]
    try:
        xs = np.array([p[0] for p in recent], dtype=float)
        ys = np.array([p[1] for p in recent], dtype=float)
    except (TypeError, ValueError) as exc:
        log.warning('[trend/disk] 数据转换失败（跳过）：%s', exc)
        return None

    try:
        slope, _ = _linear_extrapolate(xs, ys, xs[-1])
    except Exception as exc:
        log.warning('[trend/disk] 线性回归失败（跳过）：%s', exc)
        return None

    # slope 单位：bytes/second，转换为 GB/小时便于展示
    slope_gb_per_hour = slope * 3600 / (1024 ** 3)
    current_gb = current_bytes / (1024 ** 3)

    log.debug(
        '[trend/disk] 当前磁盘=%.2f GB  斜率=%.4f GB/h（阈值 1.0 GB/h）',
        current_gb, slope_gb_per_hour,
    )

    if slope <= _DISK_SLOPE_THRESHOLD:
        return None

    title = f"磁盘容量增长过快：{slope_gb_per_hour:.2f} GB/h"
    detail = (
        f"ClickHouse 活跃分区当前占用 {current_gb:.2f} GB，"
        f"近期增长斜率 {slope_gb_per_hour:.2f} GB/h，"
        f"超过告警阈值 1.0 GB/h"
    )
    log.info('[trend/disk] 容量告警触发：%s', title)

    event = AlertEvent(
        source='trend_predictor',
        category='CAPACITY',
        severity='P3',
        title=title,
        detail=detail,
        metric_name='disk_growth_rate',
        current_value=current_bytes,
        threshold_value=_DISK_SLOPE_THRESHOLD,
        affected_tables=['system.parts'],
        context={
            'slope_bytes_per_second': slope,
            'slope_gb_per_hour': slope_gb_per_hour,
            'current_bytes': current_bytes,
            'sample_points': len(recent),
        },
    )
    event.compute_fingerprint()
    return event


# ── 对外接口 ─────────────────────────────────────────────────────

def run(ch) -> list:
    """
    执行 Kafka Lag 趋势 + 磁盘容量趋势预测，返回触发的 AlertEvent 列表。
    任一子检测失败只记 warning，不中断整体流程。
    """
    alerts = []

    try:
        lag_alert = _predict_kafka_lag(ch)
        if lag_alert is not None:
            alerts.append(lag_alert)
    except Exception as exc:
        log.warning('[trend_predictor] Kafka Lag 趋势检测异常（跳过）：%s', exc)

    try:
        disk_alert = _predict_disk_growth(ch)
        if disk_alert is not None:
            alerts.append(disk_alert)
    except Exception as exc:
        log.warning('[trend_predictor] 磁盘趋势检测异常（跳过）：%s', exc)

    log.info('趋势预测完成：触发 %d 条告警', len(alerts))
    return alerts
