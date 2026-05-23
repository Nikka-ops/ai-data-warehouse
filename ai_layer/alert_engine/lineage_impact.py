# -*- coding: utf-8 -*-
"""
血缘影响评估 —— 为已有告警补充下游血缘信息，并按影响范围升级 severity。
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.logger import get_logger
from ai_layer.alert_engine import AlertEvent

log = get_logger('alert_engine.lineage_impact')

# severity 升级映射：P3→P2, P2→P1（P1/P4 不再升级）
_SEVERITY_UPGRADE: dict[str, str] = {
    'P3': 'P2',
    'P2': 'P1',
}

# 下游表数量达到此值时触发 severity 升级
_DOWNSTREAM_UPGRADE_THRESHOLD = 3


def enrich_with_lineage(alerts: list) -> list:
    """
    对每个告警的 affected_tables，用 lineage.get_downstream() 找出所有直接下游表，
    填充 alert.downstream_tables。
    下游表数量 >= _DOWNSTREAM_UPGRADE_THRESHOLD 时，severity 升级一档（P3→P2, P2→P1）。

    任何异常（import 失败、解析失败）均原样返回，不影响告警流程。
    """
    # 延迟导入，避免循环依赖，且在 lineage 模块不可用时不中断流程
    try:
        from ai_layer.lineage import get_downstream  # type: ignore
    except ImportError as exc:
        log.warning('[lineage_impact] 无法导入 lineage 模块，跳过血缘富化：%s', exc)
        return alerts

    enriched = []
    for alert in alerts:
        try:
            _enrich_single(alert, get_downstream)
        except Exception as exc:
            log.warning(
                '[lineage_impact] 告警 %r 血缘富化失败（原样保留）：%s',
                alert.title, exc,
            )
        enriched.append(alert)

    return enriched


def _enrich_single(alert: AlertEvent, get_downstream_fn) -> None:
    """
    就地修改单个 AlertEvent，填充 downstream_tables 并按需升级 severity。
    """
    downstream_set: set[str] = set()

    for table in alert.affected_tables:
        try:
            ds = get_downstream_fn(table)
            if ds:
                downstream_set.update(ds)
        except Exception as exc:
            log.debug('[lineage_impact] 查询 %s 下游失败：%s', table, exc)

    # 去掉自身表（避免自引用）
    downstream_set -= set(alert.affected_tables)
    alert.downstream_tables = sorted(downstream_set)

    downstream_count = len(alert.downstream_tables)
    log.debug(
        '[lineage_impact] 告警 %r  affected=%s  downstream=%d 个',
        alert.title, alert.affected_tables, downstream_count,
    )

    if downstream_count >= _DOWNSTREAM_UPGRADE_THRESHOLD:
        old_sev = alert.severity
        new_sev = _SEVERITY_UPGRADE.get(old_sev, old_sev)
        if new_sev != old_sev:
            alert.severity = new_sev
            log.info(
                '[lineage_impact] 告警 %r severity 升级：%s → %s（下游表 %d 个）',
                alert.title, old_sev, new_sev, downstream_count,
            )
