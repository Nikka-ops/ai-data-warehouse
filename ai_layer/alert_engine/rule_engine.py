# -*- coding: utf-8 -*-
"""
规则引擎 —— 阈值 / 空值检测
对 ClickHouse 执行预定义规则查询，产出 AlertEvent 列表。
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.logger import get_logger
from ai_layer.alert_engine import AlertEvent

log = get_logger('alert_engine.rule_engine')

# ── 规则定义（字典驱动，便于运行时扩展）────────────────────────

RULES = [
    # 取消率超过 15%（P2）
    {
        "name": "high_cancel_rate",
        "sql": (
            "SELECT round(sum(cancel_cnt)/nullIf(sum(order_cnt),0)*100,2)"
            " FROM dws.realtime_minute_stats"
            " WHERE window_start >= now() - INTERVAL 10 MINUTE"
        ),
        "threshold": 15.0,
        "op": "gt",
        "severity": "P2",
        "category": "BUSINESS",
        "title": "取消率异常偏高",
        "metric": "cancel_rate_pct",
        "affected_tables": ["dws.realtime_minute_stats"],
    },
    # 连续10分钟订单量为0（P1）
    {
        "name": "zero_order_count",
        "sql": (
            "SELECT sum(order_cnt)"
            " FROM dws.realtime_minute_stats"
            " WHERE window_start >= now() - INTERVAL 10 MINUTE"
        ),
        "threshold": 0.0,
        "op": "eq",
        "severity": "P1",
        "category": "DATA_QUALITY",
        "title": "订单流中断：10分钟内订单量为0",
        "metric": "order_cnt_10min",
        "affected_tables": ["dws.realtime_minute_stats", "ods.orders_stream"],
    },
    # Kafka 消费 Lag 超 50k（P2）
    {
        "name": "kafka_lag_high",
        "sql": (
            "SELECT max(lag)"
            " FROM stream.kappa_consumer_lag"
            " WHERE check_time >= now() - INTERVAL 5 MINUTE"
        ),
        "threshold": 50000.0,
        "op": "gt",
        "severity": "P2",
        "category": "SYSTEM",
        "title": "Kafka 消费延迟偏高",
        "metric": "kafka_lag_max",
        "affected_tables": ["stream.kappa_consumer_lag"],
    },
    # Kafka 消费 Lag 超 200k（P1）
    {
        "name": "kafka_lag_critical",
        "sql": (
            "SELECT max(lag)"
            " FROM stream.kappa_consumer_lag"
            " WHERE check_time >= now() - INTERVAL 5 MINUTE"
        ),
        "threshold": 200000.0,
        "op": "gt",
        "severity": "P1",
        "category": "SYSTEM",
        "title": "Kafka 消费严重积压",
        "metric": "kafka_lag_max",
        "affected_tables": ["stream.kappa_consumer_lag"],
    },
    # 特征陈旧（P3）
    {
        "name": "feature_stale",
        "sql": (
            "SELECT countIf(is_stale=1)"
            " FROM feature_store.feature_freshness"
        ),
        "threshold": 0.0,
        "op": "gt",
        "severity": "P3",
        "category": "DATA_QUALITY",
        "title": "存在陈旧特征",
        "metric": "stale_feature_count",
        "affected_tables": ["feature_store.feature_values"],
    },
    # GMV 连续10分钟为0（P1）
    {
        "name": "zero_gmv",
        "sql": (
            "SELECT sum(total_gmv)"
            " FROM dws.realtime_minute_stats"
            " WHERE window_start >= now() - INTERVAL 10 MINUTE"
        ),
        "threshold": 0.0,
        "op": "eq",
        "severity": "P1",
        "category": "BUSINESS",
        "title": "GMV 为零：可能存在数据断流",
        "metric": "gmv_10min",
        "affected_tables": ["dws.realtime_minute_stats"],
    },
]


# ── 工具函数 ─────────────────────────────────────────────────────

def _evaluate(value: float, threshold: float, op: str) -> bool:
    """对单个规则进行比较运算"""
    if op == "gt":
        return value > threshold
    if op == "lt":
        return value < threshold
    if op == "eq":
        return value == threshold
    if op == "ne":
        return value != threshold
    if op == "gte":
        return value >= threshold
    if op == "lte":
        return value <= threshold
    log.warning('未知比较运算符: %s，规则跳过', op)
    return False


def _run_rule(ch, rule: dict) -> AlertEvent | None:
    """
    执行单条规则。
    查询失败返回 None，不抛异常。
    触发则返回 AlertEvent，否则返回 None。
    """
    name = rule["name"]
    try:
        rows = ch.query(rule["sql"]).result_rows
        if not rows or rows[0][0] is None:
            log.debug('[%s] 查询返回空结果，跳过', name)
            return None
        value = float(rows[0][0])
    except Exception as exc:
        log.warning('[%s] 查询失败（跳过）：%s', name, exc)
        return None

    threshold = rule["threshold"]
    if not _evaluate(value, threshold, rule["op"]):
        log.debug('[%s] 未触发：value=%.4f  threshold=%.4f  op=%s',
                  name, value, threshold, rule["op"])
        return None

    log.info('[%s] 规则触发：value=%.4f  threshold=%.4f  severity=%s',
             name, value, threshold, rule["severity"])

    event = AlertEvent(
        source='rule_engine',
        category=rule["category"],
        severity=rule["severity"],
        title=rule["title"],
        detail=(
            f"规则 {name}：当前值 {value:.4g}，"
            f"阈值 {threshold:.4g}（{rule['op']}）"
        ),
        metric_name=rule["metric"],
        current_value=value,
        threshold_value=threshold,
        affected_tables=list(rule.get("affected_tables", [])),
        context={"rule_name": name, "op": rule["op"]},
    )
    event.compute_fingerprint()
    return event


# ── 对外接口 ─────────────────────────────────────────────────────

def run(ch) -> list:
    """
    执行所有规则，返回触发的 AlertEvent 列表。
    单条规则查询失败只记 warning，不中断其他规则。
    """
    results = []
    for rule in RULES:
        event = _run_rule(ch, rule)
        if event is not None:
            results.append(event)
    log.info('规则引擎执行完毕：共 %d 条规则，触发 %d 条告警', len(RULES), len(results))
    return results
