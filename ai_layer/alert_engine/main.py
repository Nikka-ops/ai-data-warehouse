#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
告警引擎主服务：触发 → 聚合 → 决策 → 修复 → 通知

启动方式：
  python -m ai_layer.alert_engine.main
  python -m ai_layer.alert_engine.main --interval 30
  python -m ai_layer.alert_engine.main --once
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import clickhouse_connect

from config import cfg
from utils.logger import get_logger
from ai_layer.alert_engine.orchestrator import AlertOrchestrator

log = get_logger('alert_engine.main')

# ── 建表 DDL ─────────────────────────────────────────────────

_DDL_AGENT_DECISION_LOG = """
CREATE TABLE IF NOT EXISTS stream.agent_decision_log (
    log_id       UUID DEFAULT generateUUIDv4(),
    log_time     DateTime DEFAULT now(),
    alert_id     String,
    alert_title  String,
    alert_severity String,
    skill_name   String,
    action_type  String,
    target       String,
    risk_level   String,
    dry_run      UInt8,
    allowed      UInt8,
    success      UInt8,
    message      String,
    resolution   String DEFAULT '',
    resolved     UInt8 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY log_time
TTL log_time + INTERVAL 90 DAY
"""

_DDL_ALERT_EVENTS = """
CREATE TABLE IF NOT EXISTS stream.alert_events (
    event_id         UUID DEFAULT generateUUIDv4(),
    received_at      DateTime DEFAULT now(),
    alert_id         String,
    source           String,
    category         String,
    severity         String,
    title            String,
    detail           String,
    metric_name      String,
    current_value    Float64,
    threshold_value  Float64,
    affected_tables  String,
    downstream_tables String,
    context          String,
    fired_at         String,
    fingerprint      String
) ENGINE = MergeTree()
ORDER BY received_at
TTL received_at + INTERVAL 90 DAY
"""


def _ensure_tables(ch):
    """自动建 stream.agent_decision_log 和 stream.alert_events"""
    for ddl in (_DDL_AGENT_DECISION_LOG, _DDL_ALERT_EVENTS):
        try:
            ch.command(ddl)
            log.debug("建表成功（或已存在）")
        except Exception as e:
            log.warning("建表失败（可能已存在，忽略）: %s", e)


def _get_ch_client():
    """创建 ClickHouse 客户端"""
    return clickhouse_connect.get_client(
        host=cfg.ch_host,
        port=cfg.ch_port,
        username=cfg.ch_user,
        password=cfg.ch_password,
    )


def run_loop(interval: int = 60):
    """
    每 interval 秒执行一轮：
    1. aggregator.run_all_detectors(ch) → alerts
    2. 对每个 alert：orchestrator.handle(alert)
    3. 日志记录本轮结果
    """
    log.info("告警引擎启动，轮询间隔 %d 秒", interval)

    # 创建 ClickHouse 连接
    try:
        ch = _get_ch_client()
        log.info("ClickHouse 连接成功 host=%s port=%s", cfg.ch_host, cfg.ch_port)
    except Exception as e:
        log.error("ClickHouse 连接失败: %s", e)
        sys.exit(1)

    # 确保表存在
    _ensure_tables(ch)

    # 构建 Orchestrator（单例，保持连接）
    orchestrator = AlertOrchestrator(ch)

    # 判断是否只运行一次（由外部设置的 flag 控制）
    run_once = getattr(run_loop, '_once', False)

    round_num = 0
    while True:
        round_num += 1
        log.info("=== 第 %d 轮告警检测开始 ===", round_num)

        alerts = []
        try:
            # 懒导入 aggregator（由另一 Agent 实现，可能不存在时优雅降级）
            from ai_layer.alert_engine import aggregator  # noqa: F401
            alerts = aggregator.run_all_detectors(ch)
            log.info("本轮检测到 %d 个告警", len(alerts))
        except ImportError:
            log.warning("aggregator 模块尚未就绪，跳过本轮检测")
        except Exception as e:
            log.error("告警检测异常: %s", e)

        # 按严重程度排序：P1 > P2 > P3 > P4
        _severity_order = {'P1': 0, 'P2': 1, 'P3': 2, 'P4': 3}
        alerts.sort(key=lambda a: _severity_order.get(a.severity, 9))

        results = []
        for alert in alerts:
            try:
                decision = orchestrator.handle(alert)
                results.append({
                    "alert_id": alert.alert_id,
                    "severity": alert.severity,
                    "title": alert.title,
                    "success": decision.get("success"),
                    "action": decision.get("action"),
                })
                log.info(
                    "告警处理完成 alert_id=%s severity=%s action=%s success=%s",
                    alert.alert_id, alert.severity,
                    decision.get("action"), decision.get("success"),
                )
            except Exception as e:
                log.error("处理告警 %s 异常: %s", alert.alert_id, e)

        # 本轮汇总
        success_cnt = sum(1 for r in results if r.get("success"))
        log.info(
            "=== 第 %d 轮完成：共 %d 个告警，%d 个处理成功 ===",
            round_num, len(results), success_cnt,
        )

        if run_once:
            log.info("--once 模式，退出循环")
            break

        log.info("等待 %d 秒后进入下一轮...", interval)
        time.sleep(interval)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='告警引擎主服务：触发 → 聚合 → 决策 → 修复 → 通知'
    )
    parser.add_argument(
        '--interval', type=int, default=60,
        help='轮询间隔（秒），默认 60',
    )
    parser.add_argument(
        '--once', action='store_true',
        help='只运行一次不循环',
    )
    args = parser.parse_args()

    if args.once:
        run_loop._once = True  # type: ignore[attr-defined]

    run_loop(args.interval)
