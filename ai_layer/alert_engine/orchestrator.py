# -*- coding: utf-8 -*-
"""
Alert Orchestrator：使用 LangGraph StateGraph 处理告警事件。
核心流程：并行诊断（diagnose + lineage + knowledge）→ 规划 → 安全检查
         → 执行修复 → 验证（带重试）→ 记录知识 → 通知
"""
import os
import sys
import json
from datetime import datetime
from typing import TypedDict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry
from ai_layer.alert_engine.skills import (
    diagnose_task,
    auto_repair,
    trace_lineage_impact,
    query_knowledge,
    write_knowledge,
)
from ai_layer.alert_engine.safety_gate import SafetyGate
from ai_layer.alert_engine.notifier import notify

from langgraph.graph import StateGraph, END

log = get_logger('alert_engine.orchestrator')

# ── LLM import ────────────────────────────────────────────────
try:
    from langchain_openai import ChatOpenAI
    _LLM_OK = True
except ImportError as _llm_err:
    log.warning("langchain_openai 导入失败，plan_action 将使用规则降级: %s", _llm_err)
    _LLM_OK = False


# ── 图状态定义 ─────────────────────────────────────────────────

class AlertState(TypedDict):
    alert: dict                  # AlertEvent 序列化为 dict
    diagnose_result: dict        # diagnose_task 输出
    lineage_result: dict         # trace_lineage_impact 输出
    knowledge_result: dict       # query_knowledge 输出
    plan: str                    # LLM 生成的处置计划（JSON 字符串）
    safety_check: dict           # 安全闸门结果
    repair_result: dict          # auto_repair 输出
    verify_result: dict          # 验证修复是否成功
    notification_sent: bool
    final_report: str            # 最终处置报告
    retry_count: int             # 重试次数
    escalated: bool              # 是否升级（超过最大重试）
    messages: list               # 中间日志


# ── 条件边函数 ─────────────────────────────────────────────────

def check_safety(state: AlertState) -> str:
    """安全闸门通过 → execute_repair，否则 → escalate"""
    if state.get("safety_check", {}).get("allowed"):
        return "execute_repair"
    return "escalate"


def should_retry(state: AlertState) -> str:
    """验证成功 → write_knowledge；重试次数未满 → plan_action；超限 → escalate"""
    if state.get("verify_result", {}).get("success"):
        return "write_knowledge"
    if state.get("retry_count", 0) < 2:
        return "plan_action"
    return "escalate"


# ── 节点函数 ───────────────────────────────────────────────────

def parallel_diagnose(state: AlertState) -> dict:
    """fan-out 入口节点，本身不做任何处理，只做路由分发"""
    return {}


def diagnose_task_node(state: AlertState) -> dict:
    """调用 diagnose_task 诊断目标健康状态"""
    alert = state["alert"]
    ch = state["_ch"]  # 通过闭包注入，见 _build_graph
    target = (
        alert.get("affected_tables", [None])[0]
        if alert.get("affected_tables")
        else alert.get("metric_name", "unknown")
    )
    msgs = list(state.get("messages", []))
    try:
        result = diagnose_task(ch, target)
        msgs.append(f"[diagnose] target={target} status={result.get('status')}")
    except Exception as e:
        result = {
            "target": target, "status": "unknown",
            "last_write": "", "row_count": 0,
            "details": f"诊断异常: {e}", "recommended_action": "trigger_etl",
        }
        msgs.append(f"[diagnose] 异常: {e}")
    return {"diagnose_result": result, "messages": msgs}


def lineage_node(state: AlertState) -> dict:
    """调用 trace_lineage_impact 评估血缘影响"""
    alert = state["alert"]
    target = (
        alert.get("affected_tables", [None])[0]
        if alert.get("affected_tables")
        else alert.get("metric_name", "unknown")
    )
    msgs = list(state.get("messages", []))
    try:
        result = trace_lineage_impact(target)
        msgs.append(f"[lineage] impact_score={result.get('impact_score', 0)}")
    except Exception as e:
        result = {
            "table": target, "upstream": [], "downstream": [],
            "impact_score": 0, "summary": f"血缘追踪异常: {e}",
        }
        msgs.append(f"[lineage] 异常: {e}")
    return {"lineage_result": result, "messages": msgs}


def knowledge_node(state: AlertState) -> dict:
    """调用 query_knowledge 检索历史案例"""
    alert = state["alert"]
    ch = state["_ch"]
    incident_desc = alert.get("title", "")
    msgs = list(state.get("messages", []))
    try:
        result = query_knowledge(ch, incident_desc)
        msgs.append(f"[knowledge] found={result.get('found')} cases={len(result.get('cases', []))}")
    except Exception as e:
        result = {"found": False, "cases": [], "suggestion": f"知识库查询异常: {e}"}
        msgs.append(f"[knowledge] 异常: {e}")
    return {"knowledge_result": result, "messages": msgs}


def join_diagnose(state: AlertState) -> dict:
    """fan-in 汇聚节点：等待三个并行子节点完成后继续"""
    msgs = list(state.get("messages", []))
    msgs.append("[join_diagnose] 并行诊断完成，准备规划")
    return {"messages": msgs}


def plan_action(state: AlertState) -> dict:
    """LLM 根据三个诊断结果生成处置计划（JSON 字符串）"""
    alert = state["alert"]
    diagnose = state.get("diagnose_result", {})
    lineage = state.get("lineage_result", {})
    knowledge = state.get("knowledge_result", {})
    msgs = list(state.get("messages", []))

    # 上次修复失败信息（重试时携带）
    last_failure = ""
    verify = state.get("verify_result", {})
    if verify and not verify.get("success") and verify.get("reason"):
        last_failure = verify["reason"]

    prompt_text = (
        "你是数据仓库运维专家。基于以下诊断信息，制定处置计划。\n\n"
        f"告警：{alert.get('severity')} - {alert.get('title')}\n"
        f"任务诊断：{json.dumps(diagnose, ensure_ascii=False)}\n"
        f"血缘影响：{json.dumps(lineage, ensure_ascii=False)}"
        f"（影响分数：{lineage.get('impact_score', 0)}）\n"
        f"历史案例：{json.dumps(knowledge, ensure_ascii=False)}\n"
        f"上次修复失败原因（若有）：{last_failure}\n\n"
        "输出 JSON（只输出 JSON，不要有其他文字）：\n"
        "{\n"
        '  "action_type": "restart_replay|trigger_etl|clear_stale_features|quarantine|noop",\n'
        '  "target": "目标表或作业名",\n'
        '  "reason": "选择此动作的原因",\n'
        '  "risk_assessment": "风险评估"\n'
        "}"
    )

    plan_str = ""
    if _LLM_OK:
        try:
            llm = ChatOpenAI(
                model=cfg.llm_model,
                api_key=cfg.api_key,
                base_url=cfg.api_base_url,
                temperature=cfg.insight_temperature,
            )
            resp = llm.invoke(prompt_text)
            plan_str = resp.content.strip()
            # 去掉可能的 markdown 代码块包裹
            if plan_str.startswith("```"):
                lines = plan_str.splitlines()
                plan_str = "\n".join(
                    line for line in lines if not line.strip().startswith("```")
                ).strip()
            msgs.append("[plan_action] LLM 规划成功")
        except Exception as e:
            msgs.append(f"[plan_action] LLM 调用失败，降级到规则: {e}")
            plan_str = ""

    # 降级：从 diagnose_result 直接推断
    if not plan_str:
        action_type = diagnose.get("recommended_action", "noop") or "noop"
        target = diagnose.get("target", alert.get("metric_name", "unknown"))
        plan_str = json.dumps({
            "action_type": action_type,
            "target": target,
            "reason": f"规则降级：诊断状态={diagnose.get('status')}，推荐动作={action_type}",
            "risk_assessment": "低风险，自动降级规则产生",
        }, ensure_ascii=False)
        msgs.append(f"[plan_action] 规则降级 action_type={action_type} target={target}")

    return {"plan": plan_str, "messages": msgs}


def safety_check_node(state: AlertState) -> dict:
    """调用 SafetyGate.check() 进行安全检查"""
    gate = state["_gate"]
    msgs = list(state.get("messages", []))

    try:
        plan = json.loads(state.get("plan", "{}"))
    except Exception:
        plan = {}

    action_type = plan.get("action_type", "noop")
    target = plan.get("target", "unknown")

    if action_type == "noop":
        result = {"allowed": True, "reason": "noop 操作无需安全检查", "action_type": action_type, "target": target}
        msgs.append("[safety_check] noop，跳过安全检查")
        return {"safety_check": result, "messages": msgs}

    try:
        allowed, reason = gate.check(action_type, target)
        result = {"allowed": allowed, "reason": reason, "action_type": action_type, "target": target}
        msgs.append(f"[safety_check] allowed={allowed} reason={reason}")
    except Exception as e:
        result = {"allowed": False, "reason": f"安全检查异常: {e}", "action_type": action_type, "target": target}
        msgs.append(f"[safety_check] 异常: {e}")

    return {"safety_check": result, "messages": msgs}


def execute_repair(state: AlertState) -> dict:
    """执行修复操作（dry_run=False）"""
    ch = state["_ch"]
    gate = state["_gate"]
    alert = state["alert"]
    safety = state.get("safety_check", {})
    msgs = list(state.get("messages", []))

    action_type = safety.get("action_type", "noop")
    target = safety.get("target", "unknown")

    if action_type == "noop":
        result = {
            "action_type": "noop", "target": target,
            "dry_run": False, "success": True,
            "message": "noop：无需修复操作", "risk_level": "low",
        }
        msgs.append("[execute_repair] noop，跳过修复")
        return {"repair_result": result, "messages": msgs}

    try:
        result = auto_repair(ch, action_type, target, dry_run=False)
        msgs.append(
            f"[execute_repair] action={action_type} target={target} "
            f"success={result.get('success')} msg={result.get('message', '')[:80]}"
        )
        # 记录执行日志
        try:
            gate.record_execution(
                action_type=action_type,
                target=target,
                alert_id=alert.get("alert_id", ""),
                success=result.get("success", False),
                dry_run=False,
                alert_title=alert.get("title", ""),
                alert_severity=alert.get("severity", ""),
                risk_level=result.get("risk_level", ""),
                allowed=True,
                message=result.get("message", ""),
            )
        except Exception as log_err:
            msgs.append(f"[execute_repair] 记录执行日志失败: {log_err}")
    except Exception as e:
        result = {
            "action_type": action_type, "target": target,
            "dry_run": False, "success": False,
            "message": f"修复执行异常: {e}", "risk_level": "unknown",
        }
        msgs.append(f"[execute_repair] 异常: {e}")

    return {"repair_result": result, "messages": msgs}


def verify_repair(state: AlertState) -> dict:
    """重新调用 diagnose_task 验证修复状态"""
    ch = state["_ch"]
    diagnose_before = state.get("diagnose_result", {})
    repair = state.get("repair_result", {})
    msgs = list(state.get("messages", []))

    target = repair.get("target") or diagnose_before.get("target", "unknown")
    status_before = diagnose_before.get("status", "unknown")

    # noop 场景：直接标记成功
    if repair.get("action_type") == "noop":
        result = {"success": True, "status_before": status_before, "status_after": status_before, "reason": "noop 操作，无需验证"}
        msgs.append("[verify_repair] noop，标记成功")
        return {"verify_result": result, "messages": msgs}

    # 修复本身失败，不必重新诊断
    if not repair.get("success"):
        result = {
            "success": False,
            "status_before": status_before,
            "status_after": "unknown",
            "reason": f"修复操作本身失败: {repair.get('message', '')}",
        }
        msgs.append("[verify_repair] 修复失败，跳过重诊断")
        return {
            "verify_result": result,
            "retry_count": state.get("retry_count", 0) + 1,
            "messages": msgs,
        }

    try:
        diag_after = diagnose_task(ch, target)
        status_after = diag_after.get("status", "unknown")
        # 成功判定：修复前为 degraded/down，修复后变为 healthy
        success = (
            status_before in ("degraded", "down")
            and status_after == "healthy"
        ) or status_after == "healthy"
        reason = (
            f"修复前状态={status_before}，修复后状态={status_after}"
            if not success
            else ""
        )
        result = {
            "success": success,
            "status_before": status_before,
            "status_after": status_after,
            "reason": reason,
        }
        msgs.append(f"[verify_repair] before={status_before} after={status_after} success={success}")
    except Exception as e:
        result = {
            "success": False,
            "status_before": status_before,
            "status_after": "unknown",
            "reason": f"验证诊断异常: {e}",
        }
        msgs.append(f"[verify_repair] 异常: {e}")

    new_retry = state.get("retry_count", 0) + (0 if result["success"] else 1)
    return {"verify_result": result, "retry_count": new_retry, "messages": msgs}


def write_knowledge_node(state: AlertState) -> dict:
    """修复成功后将处置结果写入知识库"""
    ch = state["_ch"]
    alert = state["alert"]
    diagnose = state.get("diagnose_result", {})
    lineage = state.get("lineage_result", {})
    repair = state.get("repair_result", {})
    msgs = list(state.get("messages", []))

    resolution = (
        f"[LangGraph自动处理] "
        f"诊断={diagnose.get('status')} "
        f"动作={repair.get('action_type')} "
        f"结果={'成功' if repair.get('success') else '失败'} "
        f"血缘影响={lineage.get('summary', '')}"
    )
    try:
        write_knowledge(ch, alert.get("alert_id", ""), resolution, repair.get("success", False))
        msgs.append("[write_knowledge] 写入成功")
    except Exception as e:
        msgs.append(f"[write_knowledge] 写入失败: {e}")

    return {"messages": msgs}


def notify_success(state: AlertState) -> dict:
    """修复成功后发送通知，生成最终报告"""
    alert_dict = state["alert"]
    diagnose = state.get("diagnose_result", {})
    lineage = state.get("lineage_result", {})
    knowledge = state.get("knowledge_result", {})
    repair = state.get("repair_result", {})
    verify = state.get("verify_result", {})
    msgs = list(state.get("messages", []))

    final_report = (
        f"[修复成功]\n"
        f"根因推断: {diagnose.get('details', '未知')}\n"
        f"影响范围: {lineage.get('summary', '未知')}\n"
        f"历史建议: {knowledge.get('suggestion', '无')}\n"
        f"执行动作: {repair.get('action_type', '无')}\n"
        f"执行结果: {repair.get('message', '未执行')}\n"
        f"验证状态: {verify.get('status_before', '?')} → {verify.get('status_after', '?')}"
    )

    # 构造 decision 兼容 notifier 接口
    decision = {
        "alert_id": alert_dict.get("alert_id", ""),
        "skill": "auto_repair",
        "action": repair.get("action_type", ""),
        "result": repair.get("message", "")[:200],
        "success": True,
        "report": final_report,
    }

    try:
        # 需要一个带属性的对象，用简单适配器包装 dict
        alert_obj = _DictAlert(alert_dict)
        notify(alert_obj, decision)
        msgs.append("[notify_success] 通知已发送")
    except Exception as e:
        msgs.append(f"[notify_success] 通知失败: {e}")

    return {
        "final_report": final_report,
        "notification_sent": True,
        "messages": msgs,
    }


def escalate(state: AlertState) -> dict:
    """安全检查拒绝或超过最大重试，升级处理"""
    alert_dict = state["alert"]
    safety = state.get("safety_check", {})
    verify = state.get("verify_result", {})
    msgs = list(state.get("messages", []))

    reason = (
        safety.get("reason", "")
        or verify.get("reason", "")
        or "超过最大重试次数"
    )

    final_report = (
        f"[人工升级]\n"
        f"告警: {alert_dict.get('severity')} - {alert_dict.get('title')}\n"
        f"升级原因: {reason}\n"
        f"重试次数: {state.get('retry_count', 0)}\n"
        f"诊断状态: {state.get('diagnose_result', {}).get('status', '未知')}"
    )
    msgs.append(f"[escalate] 升级处理，原因: {reason}")

    return {"escalated": True, "final_report": final_report, "messages": msgs}


def notify_escalate(state: AlertState) -> dict:
    """升级通知"""
    alert_dict = state["alert"]
    msgs = list(state.get("messages", []))

    decision = {
        "alert_id": alert_dict.get("alert_id", ""),
        "skill": "escalate",
        "action": "human_intervention",
        "result": state.get("final_report", "")[:200],
        "success": False,
        "report": state.get("final_report", ""),
    }

    try:
        alert_obj = _DictAlert(alert_dict)
        notify(alert_obj, decision)
        msgs.append("[notify_escalate] 升级通知已发送")
    except Exception as e:
        msgs.append(f"[notify_escalate] 通知失败: {e}")

    return {"notification_sent": True, "messages": msgs}


# ── 辅助适配器 ─────────────────────────────────────────────────

class _DictAlert:
    """将 alert dict 包装为带属性的对象，兼容 notifier 接口"""
    def __init__(self, d: dict):
        self.__dict__.update(d)
        # 确保列表字段存在
        if not hasattr(self, "affected_tables"):
            self.affected_tables = []
        if not hasattr(self, "downstream_tables"):
            self.downstream_tables = []
        if not hasattr(self, "fired_at"):
            self.fired_at = datetime.now()


# ── 主编排器类 ─────────────────────────────────────────────────

class AlertOrchestrator:
    def __init__(self, ch):
        self.ch = ch
        self.gate = SafetyGate(ch)
        self.graph = self._build_graph()

    def _build_graph(self):
        """构建并编译 LangGraph StateGraph"""
        ch = self.ch
        gate = self.gate

        # 为节点注入 ch 和 gate（通过 state 中的私有键）
        # 因 LangGraph 节点签名为 (state) -> dict，用闭包包装注入依赖
        def _inject(fn):
            def wrapper(state: AlertState) -> dict:
                # 注入运行时依赖
                state = dict(state)
                state["_ch"] = ch
                state["_gate"] = gate
                return fn(state)
            wrapper.__name__ = fn.__name__
            return wrapper

        builder = StateGraph(AlertState)

        # 添加所有节点
        builder.add_node("parallel_diagnose", _inject(parallel_diagnose))
        builder.add_node("diagnose_task_node", _inject(diagnose_task_node))
        builder.add_node("lineage_node", _inject(lineage_node))
        builder.add_node("knowledge_node", _inject(knowledge_node))
        builder.add_node("join_diagnose", _inject(join_diagnose))
        builder.add_node("plan_action", _inject(plan_action))
        builder.add_node("safety_check_node", _inject(safety_check_node))
        builder.add_node("execute_repair", _inject(execute_repair))
        builder.add_node("verify_repair", _inject(verify_repair))
        builder.add_node("write_knowledge", _inject(write_knowledge_node))
        builder.add_node("notify_success", _inject(notify_success))
        builder.add_node("escalate", _inject(escalate))
        builder.add_node("notify_escalate", _inject(notify_escalate))

        # 入口
        builder.set_entry_point("parallel_diagnose")

        # fan-out：parallel_diagnose → 三个并行子节点
        builder.add_edge("parallel_diagnose", "diagnose_task_node")
        builder.add_edge("parallel_diagnose", "lineage_node")
        builder.add_edge("parallel_diagnose", "knowledge_node")

        # fan-in：三个子节点 → join_diagnose
        builder.add_edge("diagnose_task_node", "join_diagnose")
        builder.add_edge("lineage_node", "join_diagnose")
        builder.add_edge("knowledge_node", "join_diagnose")

        # 主流程
        builder.add_edge("join_diagnose", "plan_action")
        builder.add_edge("plan_action", "safety_check_node")

        # 条件边：安全检查
        builder.add_conditional_edges(
            "safety_check_node",
            check_safety,
            {
                "execute_repair": "execute_repair",
                "escalate": "escalate",
            },
        )

        builder.add_edge("execute_repair", "verify_repair")

        # 条件边：验证结果
        builder.add_conditional_edges(
            "verify_repair",
            should_retry,
            {
                "write_knowledge": "write_knowledge",
                "plan_action": "plan_action",
                "escalate": "escalate",
            },
        )

        builder.add_edge("write_knowledge", "notify_success")
        builder.add_edge("notify_success", END)

        builder.add_edge("escalate", "notify_escalate")
        builder.add_edge("notify_escalate", END)

        return builder.compile()

    # ── 主处理入口 ────────────────────────────────────────────

    def handle(self, alert) -> dict:
        """
        处理单个告警，返回 final_report 和关键字段。
        保持与旧版 AlertOrchestrator.handle() 的兼容接口。
        """
        log.info(
            "[ORCHESTRATOR] 开始处理告警 alert_id=%s severity=%s title=%s",
            alert.alert_id, alert.severity, alert.title,
        )

        # 写入 alert_events 表
        try:
            self._write_alert_event(alert)
        except Exception as e:
            log.warning("写入 alert_events 失败，继续处理: %s", e)

        # 构建初始状态（_ch 和 _gate 由节点闭包注入，不放入初始 state）
        initial_state: AlertState = {
            "alert": alert.__dict__.copy() if hasattr(alert, "__dict__") else dict(alert),
            "diagnose_result": {},
            "lineage_result": {},
            "knowledge_result": {},
            "plan": "",
            "safety_check": {},
            "repair_result": {},
            "verify_result": {},
            "notification_sent": False,
            "final_report": "",
            "retry_count": 0,
            "escalated": False,
            "messages": [],
        }

        try:
            result = self.graph.invoke(initial_state)
        except Exception as e:
            log.error("[ORCHESTRATOR] LangGraph 执行异常: %s", e)
            result = {
                "final_report": f"图执行异常: {e}",
                "escalated": True,
                "repair_result": {},
                "messages": [f"图执行异常: {e}"],
            }

        log.info(
            "[ORCHESTRATOR] 告警处理完成 alert_id=%s escalated=%s",
            alert.alert_id, result.get("escalated"),
        )

        # 打印中间日志
        for msg in result.get("messages", []):
            log.debug("[STATE] %s", msg)

        return {
            "final_report": result.get("final_report", ""),
            "escalated": result.get("escalated", False),
            "repair_result": result.get("repair_result", {}),
            # 兼容旧版字段
            "alert_id": alert.alert_id,
            "skill": result.get("repair_result", {}).get("action_type", ""),
            "action": result.get("repair_result", {}).get("action_type", ""),
            "result": result.get("repair_result", {}).get("message", ""),
            "success": result.get("repair_result", {}).get("success", False),
            "report": result.get("final_report", ""),
        }

    # ── 写入 alert_events ─────────────────────────────────────

    @ch_retry
    def _write_alert_event(self, alert):
        """写入 stream.alert_events 表"""

        def esc(s) -> str:
            return str(s).replace("'", "\\'")

        affected = esc(json.dumps(list(alert.affected_tables), ensure_ascii=False))
        downstream = esc(json.dumps(list(alert.downstream_tables), ensure_ascii=False))
        context_str = esc(json.dumps(alert.context, ensure_ascii=False, default=str))

        try:
            self.ch.command(
                "INSERT INTO stream.alert_events "
                "(alert_id, source, category, severity, title, detail, "
                " metric_name, current_value, threshold_value, "
                " affected_tables, downstream_tables, context, fired_at, fingerprint) "
                "VALUES ("
                f"'{esc(alert.alert_id)}', "
                f"'{esc(alert.source)}', "
                f"'{esc(alert.category)}', "
                f"'{esc(alert.severity)}', "
                f"'{esc(alert.title)}', "
                f"'{esc(alert.detail)}', "
                f"'{esc(alert.metric_name)}', "
                f"{float(alert.current_value)}, "
                f"{float(alert.threshold_value)}, "
                f"'{affected}', "
                f"'{downstream}', "
                f"'{context_str}', "
                f"'{esc(str(alert.fired_at))}', "
                f"'{esc(alert.fingerprint)}'"
                ")"
            )
            log.debug("已写入 alert_events: alert_id=%s", alert.alert_id)
        except Exception as e:
            log.warning("写入 alert_events 失败 alert_id=%s: %s", alert.alert_id, e)
            raise
