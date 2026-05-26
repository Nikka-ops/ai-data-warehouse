# -*- coding: utf-8 -*-
"""
安全闸门：在执行修复操作前进行风险评估和限流检查。
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('alert_engine.safety_gate')

# ── 操作风险级别定义 ──────────────────────────────────────────
RISK_LEVELS: dict[str, str] = {
    'restart_replay': 'medium',
    'trigger_etl': 'low',
    'clear_stale_features': 'low',
    'quarantine': 'low',
    'restart_service': 'high',
    'drop_data': 'critical',   # 永远不自动执行
}

# ── 限流配置：每种操作每小时最多执行次数 ─────────────────────
RATE_LIMITS: dict[str, int] = {
    'low': 10,
    'medium': 3,
    'high': 1,
    'critical': 0,   # 永远不执行
}


class SafetyGate:
    def __init__(self, ch=None):
        self.ch = ch

    def check(self, action_type: str, target: str) -> tuple[bool, str]:
        """
        返回 (allowed: bool, reason: str)
        检查顺序：
        1. critical 级别 → 永远拒绝
        2. 限流：查 stream.agent_decision_log 中最近1小时执行次数
        3. Dry-run 检查：模拟 auto_repair 返回的 risk_level
        """
        risk_level = RISK_LEVELS.get(action_type, 'medium')

        # 1. critical 永远拒绝
        if risk_level == 'critical':
            reason = f"操作 '{action_type}' 为 critical 级别，永远不允许自动执行"
            log.warning("[SAFETY_GATE] BLOCKED (critical): action=%s target=%s", action_type, target)
            return False, reason

        # 2. 限流检查
        max_count = RATE_LIMITS.get(risk_level, 0)
        if max_count == 0:
            reason = f"操作 '{action_type}' 风险等级 '{risk_level}' 每小时限额为 0"
            log.warning("[SAFETY_GATE] BLOCKED (rate_limit=0): action=%s target=%s", action_type, target)
            return False, reason

        recent_count = self._count_recent_executions(action_type)
        if recent_count >= max_count:
            reason = (
                f"操作 '{action_type}' 风险等级 '{risk_level}'，"
                f"最近1小时已执行 {recent_count} 次，超出限额 {max_count} 次"
            )
            log.warning(
                "[SAFETY_GATE] BLOCKED (rate_limit): action=%s target=%s count=%d limit=%d",
                action_type, target, recent_count, max_count,
            )
            return False, reason

        # 3. high 级别额外警告（但允许执行）
        if risk_level == 'high':
            log.warning(
                "[SAFETY_GATE] WARNING: 高风险操作 action=%s target=%s，请确认已完成 dry_run 验证",
                action_type, target,
            )

        reason = (
            f"允许执行: action={action_type} target={target} "
            f"risk={risk_level} 最近1小时已执行 {recent_count}/{max_count} 次"
        )
        log.info("[SAFETY_GATE] ALLOWED: action=%s target=%s risk=%s", action_type, target, risk_level)
        return True, reason

    @ch_retry
    def _count_recent_executions(self, action_type: str) -> int:
        """查询最近1小时内该 action_type 的执行次数（allowed=1 且 dry_run=0）"""
        try:
            rows = self.ch.query(
                "SELECT count() FROM stream.agent_decision_log "
                f"WHERE action_type = '{action_type}' "
                "  AND allowed = 1 "
                "  AND dry_run = 0 "
                "  AND log_time >= now() - INTERVAL 1 HOUR"
            )
            if rows.result_rows:
                return int(rows.result_rows[0][0])
        except Exception as e:
            log.warning("查询限流计数失败 action=%s: %s", action_type, e)
        return 0

    def record_execution(
        self,
        action_type: str,
        target: str,
        alert_id: str,
        success: bool,
        dry_run: bool,
        alert_title: str = "",
        alert_severity: str = "",
        risk_level: str = "",
        allowed: bool = True,
        message: str = "",
    ):
        """写入 stream.agent_decision_log"""
        if not risk_level:
            risk_level = RISK_LEVELS.get(action_type, 'medium')
        self._write_log(
            action_type=action_type,
            target=target,
            alert_id=alert_id,
            alert_title=alert_title,
            alert_severity=alert_severity,
            risk_level=risk_level,
            dry_run=dry_run,
            allowed=allowed,
            success=success,
            message=message,
        )

    @ch_retry
    def _write_log(
        self,
        action_type: str,
        target: str,
        alert_id: str,
        alert_title: str,
        alert_severity: str,
        risk_level: str,
        dry_run: bool,
        allowed: bool,
        success: bool,
        message: str,
    ):
        # 转义单引号
        def esc(s: str) -> str:
            return str(s).replace("'", "\\'")

        try:
            self.ch.command(
                "INSERT INTO stream.agent_decision_log "
                "(alert_id, alert_title, alert_severity, skill_name, "
                " action_type, target, risk_level, dry_run, allowed, success, message) "
                "VALUES ("
                f"'{esc(alert_id)}', '{esc(alert_title)}', '{esc(alert_severity)}', "
                f"'auto_repair', "
                f"'{esc(action_type)}', '{esc(target)}', '{esc(risk_level)}', "
                f"{1 if dry_run else 0}, {1 if allowed else 0}, "
                f"{1 if success else 0}, '{esc(message)}')"
            )
            log.debug(
                "已记录执行日志: alert_id=%s action=%s dry_run=%s success=%s",
                alert_id, action_type, dry_run, success,
            )
        except Exception as e:
            log.warning("写入 agent_decision_log 失败: %s", e)
