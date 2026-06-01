# -*- coding: utf-8 -*-
"""
告警引擎可执行技能
每个技能接受 ch（ClickHouse client）及业务参数，返回结构化 dict。
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from utils.logger import get_logger
from utils.retry import ch_retry
from ai_layer.lineage import get_upstream, get_downstream

log = get_logger('alert_engine.skills')


# ─────────────────────────────────────────────────────────────
# Skill 1: diagnose_task
# ─────────────────────────────────────────────────────────────

def diagnose_task(ch, target: str) -> dict:
    """
    诊断指定表/作业的健康状态。
    target 可以是表名（如 dws.realtime_minute_stats）或作业名（如 flink-stream）。
    """
    result = {
        "target": target,
        "status": "unknown",
        "last_write": "",
        "row_count": 0,
        "details": "",
        "recommended_action": "",
    }

    # 判断是否为 Flink 作业名（不含"."，或以 flink- 开头）
    is_job = ('.' not in target) or target.startswith('flink-')

    if is_job:
        result = _diagnose_job(ch, target, result)
    else:
        result = _diagnose_table(ch, target, result)

    return result


@ch_retry
def _diagnose_table(ch, table: str, result: dict) -> dict:
    try:
        # 尝试查询最近写入时间和行数
        ch.query(
            f"SELECT count() AS cnt, max(toDateTime(now())) AS ts FROM {table} LIMIT 1"
        )
        # 更精准：如果表有时间戳字段就用，否则只看行数
        count_rows = ch.query(f"SELECT count() AS cnt FROM {table}")
        row_count = int(count_rows.first_row[0]) if count_rows.result_rows else 0

        # 尝试查最新写入时间（查 system.parts）
        db_name, tbl_name = (table.split('.', 1) + [''])[:2]
        parts_rows = ch.query(
            "SELECT max(modification_time) FROM system.parts "
            f"WHERE database = '{db_name}' AND table = '{tbl_name}'"
        )
        last_write = ""
        if parts_rows.result_rows and parts_rows.result_rows[0][0]:
            last_write = str(parts_rows.result_rows[0][0])

        result["row_count"] = row_count
        result["last_write"] = last_write

        # 判断健康状态
        if row_count == 0:
            result["status"] = "down"
            result["details"] = f"表 {table} 行数为 0，可能数据写入中断"
            result["recommended_action"] = "trigger_etl"
        elif last_write:
            try:
                lw = datetime.fromisoformat(last_write)
                now = datetime.now()
                if lw.tzinfo is not None:
                    now = datetime.now(timezone.utc)
                age_minutes = (now - lw).total_seconds() / 60
                if age_minutes > 60:
                    result["status"] = "degraded"
                    result["details"] = (
                        f"表 {table} 最近写入时间为 {last_write}，"
                        f"已超过 {age_minutes:.0f} 分钟未更新"
                    )
                    result["recommended_action"] = "restart_replay"
                else:
                    result["status"] = "healthy"
                    result["details"] = f"表 {table} 数据正常，行数 {row_count}，最近写入 {last_write}"
                    result["recommended_action"] = ""
            except Exception:
                result["status"] = "healthy"
                result["details"] = f"表 {table} 行数 {row_count}"
                result["recommended_action"] = ""
        else:
            result["status"] = "healthy"
            result["details"] = f"表 {table} 行数 {row_count}"
            result["recommended_action"] = ""

    except Exception as e:
        log.warning("诊断表 %s 失败: %s", table, e)
        result["status"] = "unknown"
        result["details"] = f"查询失败: {e}"
        result["recommended_action"] = "trigger_etl"

    return result


@ch_retry
def _diagnose_job(ch, job_name: str, result: dict) -> dict:
    try:
        rows = ch.query(
            "SELECT status, start_time, end_time, error_msg "
            "FROM stream.kappa_replay_jobs "
            f"WHERE job_name = '{job_name}' "
            "ORDER BY start_time DESC LIMIT 1"
        )
        if not rows.result_rows:
            result["status"] = "unknown"
            result["details"] = f"作业 {job_name} 在 kappa_replay_jobs 中未找到记录"
            result["recommended_action"] = "restart_replay"
        else:
            row = rows.result_rows[0]
            status_raw = str(row[0]) if row[0] else "unknown"
            start_time = str(row[1]) if row[1] else ""
            end_time = str(row[2]) if row[2] else ""
            error_msg = str(row[3]) if row[3] else ""

            result["last_write"] = end_time or start_time

            if status_raw in ("running", "RUNNING"):
                result["status"] = "healthy"
                result["details"] = f"作业 {job_name} 运行中，启动时间 {start_time}"
                result["recommended_action"] = ""
            elif status_raw in ("failed", "FAILED", "error", "ERROR"):
                result["status"] = "down"
                result["details"] = f"作业 {job_name} 失败，错误: {error_msg}"
                result["recommended_action"] = "restart_replay"
            elif status_raw in ("stopped", "STOPPED", "cancelled", "CANCELLED"):
                result["status"] = "degraded"
                result["details"] = f"作业 {job_name} 已停止"
                result["recommended_action"] = "restart_replay"
            else:
                result["status"] = "unknown"
                result["details"] = f"作业 {job_name} 状态: {status_raw}"
                result["recommended_action"] = "restart_replay"

    except Exception as e:
        log.warning("诊断作业 %s 失败: %s", job_name, e)
        result["status"] = "unknown"
        result["details"] = f"查询失败: {e}"
        result["recommended_action"] = "restart_replay"

    return result


# ─────────────────────────────────────────────────────────────
# Skill 2: auto_repair
# ─────────────────────────────────────────────────────────────

# 操作风险级别
_ACTION_RISK = {
    'restart_replay': 'medium',
    'trigger_etl': 'low',
    'clear_stale_features': 'low',
    'quarantine': 'low',
    'restart_service': 'high',
    'drop_data': 'critical',
}


def auto_repair(ch, action_type: str, target: str, dry_run: bool = True) -> dict:
    """
    自动修复操作。dry_run=True 时只模拟不执行。
    """
    risk_level = _ACTION_RISK.get(action_type, 'medium')
    result = {
        "action_type": action_type,
        "target": target,
        "dry_run": dry_run,
        "success": False,
        "message": "",
        "risk_level": risk_level,
    }

    if action_type == 'restart_replay':
        result = _repair_restart_replay(ch, target, dry_run, result)
    elif action_type == 'trigger_etl':
        result = _repair_trigger_etl(ch, target, dry_run, result)
    elif action_type == 'clear_stale_features':
        result = _repair_clear_stale_features(ch, target, dry_run, result)
    elif action_type == 'quarantine':
        result = _repair_quarantine(ch, target, dry_run, result)
    else:
        result["success"] = False
        result["message"] = f"未知操作类型: {action_type}"

    return result


@ch_retry
def _repair_restart_replay(ch, target: str, dry_run: bool, result: dict) -> dict:
    if dry_run:
        result["success"] = True
        result["message"] = (
            f"[DRY-RUN] 将在 stream.kappa_replay_jobs 插入新任务，触发 {target} 重放"
        )
        return result
    try:
        ch.command(
            "INSERT INTO stream.kappa_replay_jobs "
            "(job_name, status, start_time) VALUES "
            f"('{target}', 'pending', now())"
        )
        result["success"] = True
        result["message"] = f"已在 kappa_replay_jobs 插入重放任务，目标: {target}"
    except Exception as e:
        log.warning("restart_replay 失败 target=%s: %s", target, e)
        result["success"] = False
        result["message"] = f"写入失败: {e}"
    return result


@ch_retry
def _repair_trigger_etl(ch, target: str, dry_run: bool, result: dict) -> dict:
    if dry_run:
        result["success"] = True
        result["message"] = f"[DRY-RUN] 将写入 ETL 触发记录，目标表: {target}"
        return result
    try:
        ch.command(
            "INSERT INTO stream.system_alerts "
            "(alert_type, source, message, severity, created_at) VALUES "
            f"('ETL_TRIGGER', 'agent', 'Agent 触发 ETL 重跑: {target}', 'INFO', now())"
        )
        result["success"] = True
        result["message"] = f"已写入 ETL 触发记录，目标: {target}"
    except Exception as e:
        log.warning("trigger_etl 失败 target=%s: %s", target, e)
        result["success"] = False
        result["message"] = f"写入失败: {e}"
    return result


@ch_retry
def _repair_clear_stale_features(ch, target: str, dry_run: bool, result: dict) -> dict:
    if dry_run:
        result["success"] = True
        result["message"] = (
            f"[DRY-RUN] 将更新 feature_store.feature_definitions 重置 updated_at，目标: {target}"
        )
        return result
    try:
        ch.command(
            "ALTER TABLE feature_store.feature_definitions "
            f"UPDATE updated_at = now() WHERE feature_name = '{target}'"
        )
        result["success"] = True
        result["message"] = f"已重置 feature_definitions.updated_at，目标: {target}"
    except Exception as e:
        log.warning("clear_stale_features 失败 target=%s: %s", target, e)
        result["success"] = False
        result["message"] = f"更新失败: {e}"
    return result


@ch_retry
def _repair_quarantine(ch, target: str, dry_run: bool, result: dict) -> dict:
    if dry_run:
        result["success"] = True
        result["message"] = f"[DRY-RUN] 将在 stream.system_alerts 写入隔离记录，目标: {target}"
        return result
    try:
        ch.command(
            "INSERT INTO stream.system_alerts "
            "(alert_type, source, message, severity, created_at) VALUES "
            f"('QUARANTINE', 'agent', 'Agent 隔离目标: {target}', 'WARNING', now())"
        )
        result["success"] = True
        result["message"] = f"已写入隔离记录，目标: {target}"
    except Exception as e:
        log.warning("quarantine 失败 target=%s: %s", target, e)
        result["success"] = False
        result["message"] = f"写入失败: {e}"
    return result


# ─────────────────────────────────────────────────────────────
# Skill 3: trace_lineage_impact
# ─────────────────────────────────────────────────────────────

def trace_lineage_impact(table: str) -> dict:
    """
    用 ai_layer.lineage 查询受影响的上下游链路。
    """
    try:
        upstream = get_upstream(table)
        downstream = get_downstream(table)
    except Exception as e:
        log.warning("血缘追踪失败 table=%s: %s", table, e)
        upstream = []
        downstream = []

    impact_score = len(downstream)

    if impact_score == 0:
        summary = f"表 {table} 无下游依赖，影响范围有限。"
    elif impact_score <= 3:
        summary = (
            f"表 {table} 有 {impact_score} 个直接下游表，影响范围较小。"
            f"下游: {', '.join(downstream)}"
        )
    else:
        summary = (
            f"表 {table} 有 {impact_score} 个直接下游表，影响范围较大，需优先处理！"
            f"下游: {', '.join(downstream[:5])}{'...' if impact_score > 5 else ''}"
        )

    if upstream:
        summary += f" 上游依赖: {', '.join(upstream[:3])}{'...' if len(upstream) > 3 else ''}。"

    return {
        "table": table,
        "upstream": upstream,
        "downstream": downstream,
        "impact_score": impact_score,
        "summary": summary,
    }


# ─────────────────────────────────────────────────────────────
# Skill 4: query_knowledge / write_knowledge
# ─────────────────────────────────────────────────────────────

def query_knowledge(ch, incident_desc: str) -> dict:
    """
    在 stream.agent_decision_log 中检索历史同类告警的处置经验。
    查询 resolved=1 的记录，按 similarity 排序（用 LIKE 简单匹配 title）。
    """
    cases = []
    suggestion = ""

    # 从描述中提取关键词（取前3个词）
    keywords = [w for w in incident_desc.replace('，', ' ').replace(',', ' ').split() if len(w) >= 2][:3]

    try:
        # 构建 LIKE 条件
        if keywords:
            like_clauses = " OR ".join([f"alert_title LIKE '%{kw}%'" for kw in keywords])
            where_clause = f"resolved = 1 AND ({like_clauses})"
        else:
            where_clause = "resolved = 1"

        rows = ch.query(
            "SELECT alert_id, alert_title, alert_severity, action_type, "
            "       target, resolution, log_time "
            "FROM stream.agent_decision_log "
            f"WHERE {where_clause} "
            "ORDER BY log_time DESC LIMIT 3"
        )

        for row in (rows.result_rows or []):
            cases.append({
                "alert_id": str(row[0]),
                "alert_title": str(row[1]),
                "severity": str(row[2]),
                "action_type": str(row[3]),
                "target": str(row[4]),
                "resolution": str(row[5]),
                "log_time": str(row[6]),
            })

        if cases:
            actions = [c["action_type"] for c in cases if c["action_type"]]
            most_common = max(set(actions), key=actions.count) if actions else ""
            suggestion = (
                f"找到 {len(cases)} 条历史案例，最常用的修复动作为 '{most_common}'。"
                f"建议参考历史处置经验：{cases[0]['resolution']}"
                if cases[0]['resolution'] else
                f"找到 {len(cases)} 条历史案例，最常用的修复动作为 '{most_common}'。"
            )
        else:
            suggestion = "未找到类似历史案例，建议人工研判。"

    except Exception as e:
        log.warning("查询知识库失败: %s", e)
        suggestion = f"知识库查询异常: {e}"

    return {
        "found": len(cases) > 0,
        "cases": cases,
        "suggestion": suggestion,
    }


def write_knowledge(ch, alert_id: str, resolution: str, success: bool):
    """
    告警处置完成后，将结果写入 stream.agent_decision_log（更新 resolution 字段）。
    """
    try:
        ch.command(
            "ALTER TABLE stream.agent_decision_log "
            f"UPDATE resolution = '{resolution}', "
            f"       resolved = {1 if success else 0} "
            f"WHERE alert_id = '{alert_id}'"
        )
        log.info("已更新知识库记录 alert_id=%s resolved=%s", alert_id, success)
    except Exception as e:
        log.warning("写入知识库失败 alert_id=%s: %s", alert_id, e)
