# -*- coding: utf-8 -*-
"""
告警聚合器 —— 去重 / 静默 / 优先级排序
提供全局单例 get_aggregator()，以及 run_all_detectors() 一键执行所有检测器。
"""

import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from datetime import datetime, timedelta

from utils.logger import get_logger
from ai_layer.alert_engine import AlertEvent

log = get_logger('alert_engine.aggregator')

# severity 优先级（数值越小优先级越高）
_SEVERITY_ORDER: dict[str, int] = {
    'P1': 1,
    'P2': 2,
    'P3': 3,
    'P4': 4,
}


class AlertAggregator:
    """
    告警聚合器：去重、静默、优先级排序。

    Args:
        dedup_window_seconds: 相同 fingerprint 的告警在此时间窗口内只保留一条（默认15分钟）。
    """

    def __init__(self, dedup_window_seconds: int = 900):
        self._dedup_window = timedelta(seconds=dedup_window_seconds)
        self._seen: dict[str, datetime] = {}         # fingerprint → 上次触发时间
        self._silence_rules: list[dict] = []         # 静默规则列表

    # ── 静默规则管理 ─────────────────────────────────────────────

    def add_silence(
        self,
        source: str = '',
        metric: str = '',
        duration_seconds: int = 3600,
    ) -> None:
        """
        添加静默规则。
        匹配条件（source / metric 任一非空时生效）：
          - source 非空：alert.source == source
          - metric 非空：alert.metric_name 包含 metric（子串匹配）
        两个条件同时非空时取 AND 语义。

        Args:
            source:           来源过滤（'rule_engine'/'anomaly_detector' 等），空串表示不过滤
            metric:           指标名过滤（子串匹配），空串表示不过滤
            duration_seconds: 静默持续时长（秒），默认1小时
        """
        expires_at = datetime.now() + timedelta(seconds=duration_seconds)
        rule = {
            'source': source,
            'metric': metric,
            'expires_at': expires_at,
        }
        self._silence_rules.append(rule)
        log.info(
            '[aggregator] 添加静默规则：source=%r  metric=%r  expires_at=%s',
            source, metric, expires_at.strftime('%Y-%m-%d %H:%M:%S'),
        )

    def _is_silenced(self, alert: AlertEvent) -> bool:
        """判断告警是否命中任一有效静默规则"""
        now = datetime.now()
        for rule in self._silence_rules:
            # 过期的规则直接跳过
            if rule['expires_at'] <= now:
                continue

            source_match = (not rule['source']) or (alert.source == rule['source'])
            metric_match = (not rule['metric']) or (rule['metric'] in alert.metric_name)

            if source_match and metric_match:
                log.debug(
                    '[aggregator] 告警 %r 命中静默规则（source=%r metric=%r）',
                    alert.title, rule['source'], rule['metric'],
                )
                return True
        return False

    def _cleanup_expired_silence(self) -> None:
        """清理已过期的静默规则（惰性清理，每次 process 调用时执行）"""
        now = datetime.now()
        before = len(self._silence_rules)
        self._silence_rules = [r for r in self._silence_rules if r['expires_at'] > now]
        removed = before - len(self._silence_rules)
        if removed:
            log.debug('[aggregator] 清理过期静默规则 %d 条', removed)

    # ── 核心聚合流程 ─────────────────────────────────────────────

    def process(self, alerts: list) -> list:
        """
        对输入的告警列表执行：
          1. compute_fingerprint（确保 fingerprint 已填充）
          2. 去重：同一 fingerprint 在 dedup_window 内已出现则丢弃
          3. 静默：命中静默规则则丢弃
          4. 优先级排序：P1 > P2 > P3 > P4
          5. 填充 alert_id（UUID4）

        Returns:
            去重、静默后的告警列表（已按 severity 排序）。
        """
        self._cleanup_expired_silence()
        now = datetime.now()
        accepted: list[AlertEvent] = []

        for alert in alerts:
            # Step 1: 确保 fingerprint
            if not alert.fingerprint:
                alert.compute_fingerprint()

            fp = alert.fingerprint

            # Step 2: 去重
            last_fired = self._seen.get(fp)
            if last_fired is not None and (now - last_fired) < self._dedup_window:
                log.debug(
                    '[aggregator] 去重丢弃：fingerprint=%s  title=%r  last_fired=%s',
                    fp, alert.title, last_fired.strftime('%H:%M:%S'),
                )
                continue

            # Step 3: 静默
            if self._is_silenced(alert):
                continue

            # Step 4: 更新去重记录
            self._seen[fp] = now

            # Step 5: 填充 alert_id
            alert.alert_id = str(uuid.uuid4())

            accepted.append(alert)
            log.info(
                '[aggregator] 接受告警：[%s] %s  fingerprint=%s',
                alert.severity, alert.title, fp,
            )

        # Step 6: 优先级排序（P1 最高）
        accepted.sort(key=lambda a: _SEVERITY_ORDER.get(a.severity, 99))

        log.info(
            '[aggregator] 聚合完成：输入 %d 条，输出 %d 条',
            len(alerts), len(accepted),
        )
        return accepted

    # ── 一键执行所有检测器 ───────────────────────────────────────

    def run_all_detectors(self, ch) -> list:
        """
        依次调用 rule_engine、anomaly_detector、trend_predictor，
        用 lineage_impact 补充血缘信息，最后经 process() 去重/静默/排序。

        任何单个 detector 失败只记 warning，不中断整体流程。

        Args:
            ch: ClickHouse 客户端实例（clickhouse_connect.Client）

        Returns:
            处理后的告警列表（已去重、静默、排序）
        """
        raw_alerts: list[AlertEvent] = []

        # rule_engine
        try:
            from ai_layer.alert_engine import rule_engine  # type: ignore
            alerts = rule_engine.run(ch)
            raw_alerts.extend(alerts)
            log.info('[aggregator] rule_engine 完成，产出 %d 条', len(alerts))
        except Exception as exc:
            log.warning('[aggregator] rule_engine 执行失败（跳过）：%s', exc)

        # anomaly_detector
        try:
            from ai_layer.alert_engine import anomaly_detector  # type: ignore
            alerts = anomaly_detector.run(ch)
            raw_alerts.extend(alerts)
            log.info('[aggregator] anomaly_detector 完成，产出 %d 条', len(alerts))
        except Exception as exc:
            log.warning('[aggregator] anomaly_detector 执行失败（跳过）：%s', exc)

        # trend_predictor
        try:
            from ai_layer.alert_engine import trend_predictor  # type: ignore
            alerts = trend_predictor.run(ch)
            raw_alerts.extend(alerts)
            log.info('[aggregator] trend_predictor 完成，产出 %d 条', len(alerts))
        except Exception as exc:
            log.warning('[aggregator] trend_predictor 执行失败（跳过）：%s', exc)

        # lineage_impact 血缘富化
        try:
            from ai_layer.alert_engine import lineage_impact  # type: ignore
            raw_alerts = lineage_impact.enrich_with_lineage(raw_alerts)
            log.info('[aggregator] lineage_impact 富化完成')
        except Exception as exc:
            log.warning('[aggregator] lineage_impact 富化失败（跳过）：%s', exc)

        # 聚合：去重、静默、排序
        return self.process(raw_alerts)


# ── 全局单例 ─────────────────────────────────────────────────────

_aggregator_instance: AlertAggregator | None = None


def get_aggregator(dedup_window_seconds: int = 900) -> AlertAggregator:
    """
    返回全局 AlertAggregator 单例。
    首次调用时以 dedup_window_seconds 初始化；后续调用忽略该参数，直接返回已有实例。
    """
    global _aggregator_instance
    if _aggregator_instance is None:
        _aggregator_instance = AlertAggregator(dedup_window_seconds=dedup_window_seconds)
        log.info(
            '[aggregator] 创建全局单例，去重窗口=%d 秒', dedup_window_seconds
        )
    return _aggregator_instance
