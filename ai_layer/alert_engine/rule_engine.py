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
    # 执行来自 business_monitor 的环比告警规则
    for fn in (rule_gmv_yoy_drop, rule_cancel_rate_yoy_delta, rule_category_yoy_drop):
        try:
            events = fn(ch)
            results.extend(events)
        except Exception as exc:
            log.warning('[business_rule] %s 执行失败（跳过）：%s', fn.__name__, exc)
    log.info('规则引擎执行完毕：共 %d 条规则，触发 %d 条告警', len(RULES), len(results))
    return results


# ── 来自 business_monitor.py 的环比告警规则 ──────────────────────
# 对比昨天同时段（-25h ~ -23h），检测 GMV/取消率/品类的环比异常。
# 阈值与 business_monitor 保持一致，避免重复配置。

_GMV_DROP_THRESHOLD      = 0.20   # GMV 下跌超过 20% 触发
_CANCEL_RATE_ABS         = 0.30   # 取消率绝对值超过 30%
_CANCEL_RATE_DELTA       = 0.10   # 取消率比昨天高 10pp
_CATEGORY_DROP_THRESHOLD = 0.40   # 品类订单量下跌超过 40%


def _fetch_agg(ch, interval_start: str, interval_end: str) -> dict:
    """查询指定时间段的 GMV / 订单量 / 取消数汇总"""
    rows = ch.query(f"""
        SELECT sum(total_gmv), sum(order_cnt), sum(cancel_cnt)
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL {interval_start}
          AND window_start <  now() - INTERVAL {interval_end}
    """).result_rows
    if not rows or rows[0][0] is None:
        return {'gmv': 0.0, 'order_cnt': 0, 'cancel_cnt': 0}
    r = rows[0]
    return {'gmv': float(r[0] or 0), 'order_cnt': int(r[1] or 0), 'cancel_cnt': int(r[2] or 0)}


def _fetch_category_agg(ch, interval_start: str, interval_end: str) -> dict:
    """查询指定时间段各品类订单量"""
    rows = ch.query(f"""
        SELECT product_category, sum(order_cnt)
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL {interval_start}
          AND window_start <  now() - INTERVAL {interval_end}
        GROUP BY product_category
    """).result_rows
    return {r[0]: int(r[1] or 0) for r in rows if r[0]}


def rule_gmv_yoy_drop(ch) -> list:
    """GMV 环比昨天同时段下跌超过 20% → P2 告警"""
    cur  = _fetch_agg(ch, '1 HOUR', '0 SECOND')
    base = _fetch_agg(ch, '25 HOUR', '23 HOUR')
    if base['gmv'] <= 0:
        return []

    change_pct = (cur['gmv'] - base['gmv']) / base['gmv'] * 100
    if change_pct >= -_GMV_DROP_THRESHOLD * 100:
        return []

    severity = 'P1' if change_pct < -35 else 'P2'
    detail   = (f"当前1小时 GMV={cur['gmv']:.2f}，昨天同期={base['gmv']:.2f}，"
                f"环比下跌 {abs(change_pct):.1f}%")
    log.warning('[business_rule] GMV 环比告警：%s', detail)

    event = AlertEvent(
        source='rule_engine',
        category='BUSINESS',
        severity=severity,
        title='GMV 环比昨天同期下跌',
        detail=detail,
        metric_name='gmv_yoy_change_pct',
        current_value=cur['gmv'],
        threshold_value=base['gmv'],
        affected_tables=['dws.realtime_minute_stats'],
        context={'change_pct': change_pct, 'baseline_period': 'yesterday_same_hour'},
    )
    event.compute_fingerprint()
    return [event]


def rule_cancel_rate_yoy_delta(ch) -> list:
    """取消率超过 30% 且比昨天同期高 10pp → P2 告警（环比维度，补充 high_cancel_rate 绝对值规则）"""
    cur  = _fetch_agg(ch, '1 HOUR', '0 SECOND')
    base = _fetch_agg(ch, '25 HOUR', '23 HOUR')
    if cur['order_cnt'] <= 0:
        return []

    cur_rate  = cur['cancel_cnt'] / cur['order_cnt']
    base_rate = (base['cancel_cnt'] / base['order_cnt']) if base['order_cnt'] > 0 else 0.0
    delta_pp  = (cur_rate - base_rate) * 100

    # 同时满足绝对阈值和环比阈值才触发
    if cur_rate < _CANCEL_RATE_ABS or delta_pp < _CANCEL_RATE_DELTA * 100:
        return []

    severity = 'P1' if cur_rate > 0.50 else 'P2'
    detail   = (f"当前取消率={cur_rate:.1%}，昨天同期={base_rate:.1%}，"
                f"高出 {delta_pp:.1f}pp（阈值 {_CANCEL_RATE_DELTA*100:.0f}pp）")
    log.warning('[business_rule] 取消率环比告警：%s', detail)

    event = AlertEvent(
        source='rule_engine',
        category='BUSINESS',
        severity=severity,
        title='订单取消率环比异常偏高',
        detail=detail,
        metric_name='cancel_rate_yoy_delta_pp',
        current_value=cur_rate * 100,
        threshold_value=base_rate * 100,
        affected_tables=['dws.realtime_minute_stats'],
        context={'delta_pp': delta_pp, 'abs_threshold': _CANCEL_RATE_ABS,
                 'baseline_period': 'yesterday_same_hour'},
    )
    event.compute_fingerprint()
    return [event]


def rule_category_yoy_drop(ch) -> list:
    """任意品类订单量环比昨天同期下跌超过 40% → P2 告警"""
    cur_cats  = _fetch_category_agg(ch, '1 HOUR', '0 SECOND')
    base_cats = _fetch_category_agg(ch, '25 HOUR', '23 HOUR')
    events = []

    for cat, base_cnt in base_cats.items():
        if base_cnt <= 0:
            continue
        cur_cnt    = cur_cats.get(cat, 0)
        change_pct = (cur_cnt - base_cnt) / base_cnt * 100
        if change_pct >= -_CATEGORY_DROP_THRESHOLD * 100:
            continue

        severity = 'P1' if change_pct < -60 else 'P2'
        detail   = (f"品类【{cat}】当前1小时订单量={cur_cnt}，"
                    f"昨天同期={base_cnt}，环比下跌 {abs(change_pct):.1f}%")
        log.warning('[business_rule] 品类环比告警：%s', detail)

        event = AlertEvent(
            source='rule_engine',
            category='BUSINESS',
            severity=severity,
            title=f'品类订单量环比下跌：{cat}',
            detail=detail,
            metric_name=f'category_order_yoy_drop:{cat}',
            current_value=float(cur_cnt),
            threshold_value=float(base_cnt),
            affected_tables=['dws.realtime_minute_stats'],
            context={'category': cat, 'change_pct': change_pct,
                     'baseline_period': 'yesterday_same_hour'},
        )
        event.compute_fingerprint()
        events.append(event)

    return events


# ── 来自 alert_investigator.py 的 Kafka Lag / ETL 质量独有检测逻辑 ──
# alert_investigator 按 consumer_group+topic 粒度检测 Lag，
# 与 RULES 中仅取全局 max(lag) 的规则互补。

_KAFKA_LAG_HIGH = 50_000    # 超过此值为 HIGH
_KAFKA_LAG_CRIT = 200_000   # 超过此值为 CRITICAL
_ETL_SCORE_WARN = 70.0      # ETL 质量分低于此值告警
_ETL_SCORE_HIGH = 50.0      # ETL 质量分低于此值升级为 HIGH


def rule_kafka_lag_by_group(ch) -> list:
    """按 consumer_group + topic 粒度检测 Kafka Lag（仅生产消费，排除回放）"""
    try:
        rows = ch.query("""
            SELECT consumer_group, topic, max(lag) AS max_lag, avg(lag) AS avg_lag
            FROM stream.kappa_consumer_lag
            WHERE check_time >= now() - INTERVAL 5 MINUTE
              AND is_replay = 0
            GROUP BY consumer_group, topic
            HAVING max_lag > %(threshold)s
        """ % {'threshold': _KAFKA_LAG_HIGH}).result_rows
    except Exception as exc:
        log.warning('[kafka_lag_by_group] 查询失败（跳过）：%s', exc)
        return []

    events = []
    for r in rows:
        max_lag  = int(r[2] or 0)
        severity = 'P1' if max_lag > _KAFKA_LAG_CRIT else 'P2'
        detail   = (f"消费组 {r[0]} 主题 {r[1]} Lag 峰值 {max_lag:,}，"
                    f"均值 {int(r[3] or 0):,}，阈值 {_KAFKA_LAG_HIGH:,}")
        log.warning('[business_rule] Kafka Lag 分组告警：%s', detail)

        event = AlertEvent(
            source='rule_engine',
            category='SYSTEM',
            severity=severity,
            title=f'Kafka Lag 过高：{r[1]}（{r[0]}）',
            detail=detail,
            metric_name='kafka_lag_by_group',
            current_value=float(max_lag),
            threshold_value=float(_KAFKA_LAG_HIGH),
            affected_tables=['stream.kappa_consumer_lag'],
            context={'consumer_group': r[0], 'topic': r[1], 'avg_lag': int(r[3] or 0)},
        )
        event.compute_fingerprint()
        events.append(event)
    return events


def rule_etl_quality_degradation(ch) -> list:
    """ETL 平均质量分低于阈值告警（近30分钟）"""
    try:
        rows = ch.query("""
            SELECT round(avg(quality_score), 1) AS avg_score, count() AS runs
            FROM stream.etl_audit_log
            WHERE run_time >= now() - INTERVAL 30 MINUTE
        """).result_rows
    except Exception as exc:
        log.warning('[etl_quality] 查询失败（跳过）：%s', exc)
        return []

    if not rows or not rows[0][1] or int(rows[0][1]) == 0:
        return []

    avg_score = float(rows[0][0] or 100)
    runs      = int(rows[0][1])
    if avg_score >= _ETL_SCORE_WARN:
        return []

    severity = 'P2' if avg_score < _ETL_SCORE_HIGH else 'P3'
    detail   = (f"近30分钟 ETL 平均质量分 {avg_score:.1f}/100（{runs} 次运行），"
                f"低于阈值 {_ETL_SCORE_WARN}")
    log.warning('[business_rule] ETL 质量告警：%s', detail)

    event = AlertEvent(
        source='rule_engine',
        category='DATA_QUALITY',
        severity=severity,
        title=f'ETL 质量分下降：{avg_score:.1f}/100',
        detail=detail,
        metric_name='etl_avg_quality_score',
        current_value=avg_score,
        threshold_value=_ETL_SCORE_WARN,
        affected_tables=['stream.etl_audit_log'],
        context={'run_count': runs},
    )
    event.compute_fingerprint()
    return [event]
