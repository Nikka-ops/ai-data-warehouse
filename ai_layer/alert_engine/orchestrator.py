# -*- coding: utf-8 -*-
"""
Alert Orchestrator：使用 LangChain ReAct 模式处理告警事件。
核心流程：诊断 → 查历史 → 评估影响 → 修复（dry_run → 执行）→ 记录知识
"""
import os
import sys
import json
from datetime import datetime

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

log = get_logger('alert_engine.orchestrator')

# ── LangChain imports ─────────────────────────────────────────
try:
    from langchain.agents import AgentExecutor, create_react_agent
    from langchain.tools import tool
    from langchain_openai import ChatOpenAI
    from langchain.prompts import PromptTemplate
    _LANGCHAIN_OK = True
except ImportError as _lc_err:
    log.warning("LangChain 导入失败，将使用降级模式: %s", _lc_err)
    _LANGCHAIN_OK = False

# ── ReAct System Prompt ───────────────────────────────────────
SYSTEM_PROMPT = """你是一个数据仓库运维 Agent。你会收到一个告警事件，需要：
1. 调用 diagnose_task 诊断受影响的表/作业
2. 调用 query_knowledge 检索历史案例
3. 调用 trace_lineage_impact 评估影响范围
4. 根据诊断结果，选择合适的 auto_repair 动作（先 dry_run=True 验证，再 dry_run=False 执行）
5. 调用 write_knowledge 记录处置结果

输出最终决策报告，包含：根因推断、影响范围、执行动作、结果。"""

REACT_PROMPT_TEMPLATE = """你是一个数据仓库运维 Agent，负责自动诊断和修复告警问题。

可用工具:
{tools}

工具名称列表: {tool_names}

处理告警的步骤：
1. 先用 diagnose_task 诊断受影响的表或作业健康状态
2. 用 query_knowledge 检索历史相似案例
3. 用 trace_lineage_impact 评估影响范围
4. 根据诊断建议，先 dry_run=True 调用 auto_repair 验证，通过后 dry_run=False 执行
5. 最后调用 write_knowledge 记录处置结果

格式要求（严格遵守）:
Thought: 我需要先...
Action: 工具名称
Action Input: 参数（JSON 格式）
Observation: 工具返回结果
... (重复 Thought/Action/Observation)
Thought: 我已完成所有步骤
Final Answer: 决策报告，包含根因推断、影响范围、执行动作、最终结果

当前告警信息：
{input}

{agent_scratchpad}"""


class AlertOrchestrator:
    def __init__(self, ch):
        self.ch = ch
        self.gate = SafetyGate(ch)
        self._agent_executor = None
        if _LANGCHAIN_OK:
            self._agent_executor = self._build_agent()

    # ── Agent 构建 ────────────────────────────────────────────

    def _build_agent(self):
        """构建 LangChain ReAct Agent，将 Skills 包装为 LangChain Tools"""
        ch = self.ch
        gate = self.gate

        @tool
        def diagnose_task_tool(target: str) -> str:
            """
            诊断指定表或 Flink 作业的健康状态。
            参数: target - 表名（如 dws.realtime_minute_stats）或作业名（如 flink-stream）
            返回: JSON 字符串，含 status/last_write/row_count/details/recommended_action
            """
            result = diagnose_task(ch, target)
            return json.dumps(result, ensure_ascii=False, default=str)

        @tool
        def query_knowledge_tool(incident_desc: str) -> str:
            """
            在历史告警处置记录中检索相似案例。
            参数: incident_desc - 告警描述或关键词
            返回: JSON 字符串，含 found/cases/suggestion
            """
            result = query_knowledge(ch, incident_desc)
            return json.dumps(result, ensure_ascii=False, default=str)

        @tool
        def trace_lineage_impact_tool(table: str) -> str:
            """
            追踪指定表的血缘链路，评估影响范围。
            参数: table - 表名（如 dws.realtime_minute_stats）
            返回: JSON 字符串，含 upstream/downstream/impact_score/summary
            """
            result = trace_lineage_impact(table)
            return json.dumps(result, ensure_ascii=False, default=str)

        @tool
        def auto_repair_tool(action_type: str, target: str, dry_run: bool = True) -> str:
            """
            执行自动修复操作，内含安全闸门检查。
            参数:
              action_type - 操作类型: restart_replay/trigger_etl/clear_stale_features/quarantine
              target - 目标表名或作业名
              dry_run - True=模拟不执行, False=真实执行（默认 True）
            返回: JSON 字符串，含 success/message/risk_level
            """
            # 安全闸门检查（只在真实执行时）
            if not dry_run:
                allowed, reason = gate.check(action_type, target)
                if not allowed:
                    result = {
                        "action_type": action_type,
                        "target": target,
                        "dry_run": dry_run,
                        "success": False,
                        "message": f"安全闸门拒绝: {reason}",
                        "risk_level": "blocked",
                    }
                    log.warning("[ORCHESTRATOR] 安全闸门拒绝执行: %s -> %s", action_type, target)
                    return json.dumps(result, ensure_ascii=False)

            result = auto_repair(ch, action_type, target, dry_run=dry_run)

            # 记录执行日志
            gate.record_execution(
                action_type=action_type,
                target=target,
                alert_id="",
                success=result.get("success", False),
                dry_run=dry_run,
                risk_level=result.get("risk_level", ""),
                allowed=True,
                message=result.get("message", ""),
            )
            return json.dumps(result, ensure_ascii=False, default=str)

        @tool
        def write_knowledge_tool(alert_id: str, resolution: str, success: bool) -> str:
            """
            将告警处置结果写入知识库，供未来检索。
            参数:
              alert_id - 告警 ID
              resolution - 处置说明（根因 + 执行动作 + 结果）
              success - 是否成功解决
            返回: "OK" 或错误信息
            """
            try:
                write_knowledge(ch, alert_id, resolution, success)
                return "OK"
            except Exception as e:
                return f"写入失败: {e}"

        tools = [
            diagnose_task_tool,
            query_knowledge_tool,
            trace_lineage_impact_tool,
            auto_repair_tool,
            write_knowledge_tool,
        ]

        llm = ChatOpenAI(
            model=cfg.llm_model,
            api_key=cfg.api_key,
            base_url=cfg.api_base_url,
            temperature=cfg.insight_temperature,
        )

        prompt = PromptTemplate.from_template(REACT_PROMPT_TEMPLATE)

        agent = create_react_agent(llm=llm, tools=tools, prompt=prompt)
        executor = AgentExecutor(
            agent=agent,
            tools=tools,
            verbose=True,
            max_iterations=12,
            handle_parsing_errors=True,
            return_intermediate_steps=True,
        )
        return executor

    # ── 主处理入口 ────────────────────────────────────────────

    def handle(self, alert) -> dict:
        """
        处理单个告警，返回决策结果 dict。
        先写入 stream.alert_events，再运行 Agent，最后通知。
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

        decision = {
            "alert_id": alert.alert_id,
            "skill": "",
            "action": "",
            "result": "",
            "success": False,
            "report": "",
        }

        # 构建告警描述传给 Agent
        alert_input = self._format_alert_input(alert)

        if self._agent_executor is not None:
            decision = self._run_agent(alert, alert_input, decision)
        else:
            decision = self._run_fallback(alert, decision)

        # 发送通知
        try:
            notify(alert, decision)
        except Exception as e:
            log.warning("通知发送失败，继续: %s", e)

        log.info(
            "[ORCHESTRATOR] 告警处理完成 alert_id=%s success=%s",
            alert.alert_id, decision.get("success"),
        )
        return decision

    def _format_alert_input(self, alert) -> str:
        """将 AlertEvent 格式化为 Agent 输入字符串"""
        affected = ', '.join(alert.affected_tables) if alert.affected_tables else '无'
        downstream = ', '.join(alert.downstream_tables) if alert.downstream_tables else '无'
        return (
            f"告警ID: {alert.alert_id}\n"
            f"来源: {alert.source}\n"
            f"类别: {alert.category}\n"
            f"严重程度: {alert.severity}\n"
            f"标题: {alert.title}\n"
            f"详情: {alert.detail}\n"
            f"指标名称: {alert.metric_name}\n"
            f"当前值: {alert.current_value}\n"
            f"阈值: {alert.threshold_value}\n"
            f"受影响表: {affected}\n"
            f"下游影响表: {downstream}\n"
            f"触发时间: {alert.fired_at}\n"
            f"指纹: {alert.fingerprint}"
        )

    def _run_agent(self, alert, alert_input: str, decision: dict) -> dict:
        """运行 LangChain ReAct Agent"""
        try:
            result = self._agent_executor.invoke({"input": alert_input})
            final_answer = result.get("output", "")

            # 从中间步骤提取执行的技能和动作
            skill_used = ""
            action_used = ""
            action_success = False

            for step in result.get("intermediate_steps", []):
                if not step:
                    continue
                agent_action = step[0] if isinstance(step, (list, tuple)) else step
                tool_name = getattr(agent_action, "tool", "")
                tool_input = getattr(agent_action, "tool_input", {})

                if tool_name == "auto_repair_tool":
                    skill_used = "auto_repair"
                    if isinstance(tool_input, dict):
                        action_used = tool_input.get("action_type", "")
                    # 解析工具输出
                    tool_output = step[1] if isinstance(step, (list, tuple)) and len(step) > 1 else ""
                    try:
                        out = json.loads(tool_output) if isinstance(tool_output, str) else {}
                        if out.get("success"):
                            action_success = True
                    except Exception:
                        pass

            decision.update({
                "skill": skill_used or "diagnose_task",
                "action": action_used or "diagnose",
                "result": final_answer[:500] if final_answer else "Agent 处理完成",
                "success": action_success or ("成功" in final_answer or "完成" in final_answer),
                "report": final_answer,
            })

        except Exception as e:
            log.warning("[ORCHESTRATOR] Agent 执行异常，降级处理: %s", e)
            decision = self._run_fallback(alert, decision)

        return decision

    def _run_fallback(self, alert, decision: dict) -> dict:
        """降级处理：无 LangChain 时直接调用技能"""
        log.info("[ORCHESTRATOR] 使用降级模式处理告警 alert_id=%s", alert.alert_id)

        # Step 1: 诊断
        target = alert.affected_tables[0] if alert.affected_tables else alert.metric_name
        diag = diagnose_task(self.ch, target)
        log.info("[FALLBACK] 诊断结果: status=%s recommended=%s", diag["status"], diag["recommended_action"])

        # Step 2: 查历史
        knowledge = query_knowledge(self.ch, alert.title)
        log.info("[FALLBACK] 历史案例: found=%s suggestion=%s", knowledge["found"], knowledge["suggestion"])

        # Step 3: 血缘影响
        lineage = trace_lineage_impact(target)
        log.info("[FALLBACK] 血缘影响: impact_score=%d", lineage["impact_score"])

        # Step 4: 执行修复
        action_type = diag.get("recommended_action", "")
        repair_result = {}
        action_success = False

        if action_type:
            # 安全闸门检查
            allowed, reason = self.gate.check(action_type, target)
            if allowed:
                # dry_run 先验证
                dry_result = auto_repair(self.ch, action_type, target, dry_run=True)
                log.info("[FALLBACK] dry_run 结果: %s", dry_result["message"])

                # 真实执行
                repair_result = auto_repair(self.ch, action_type, target, dry_run=False)
                action_success = repair_result.get("success", False)

                # 记录日志
                self.gate.record_execution(
                    action_type=action_type,
                    target=target,
                    alert_id=alert.alert_id,
                    success=action_success,
                    dry_run=False,
                    alert_title=alert.title,
                    alert_severity=alert.severity,
                    risk_level=repair_result.get("risk_level", ""),
                    allowed=True,
                    message=repair_result.get("message", ""),
                )
            else:
                log.warning("[FALLBACK] 安全闸门拒绝: %s", reason)
                repair_result = {"message": f"安全闸门拒绝: {reason}", "success": False}

        # Step 5: 记录知识
        resolution = (
            f"[自动处理] 诊断={diag['status']} 动作={action_type} "
            f"结果={'成功' if action_success else '失败'} "
            f"血缘影响={lineage['summary']}"
        )
        write_knowledge(self.ch, alert.alert_id, resolution, action_success)

        decision.update({
            "skill": "auto_repair" if action_type else "diagnose_task",
            "action": action_type or "diagnose",
            "result": repair_result.get("message", diag["details"]),
            "success": action_success,
            "report": (
                f"根因推断: {diag['details']}\n"
                f"影响范围: {lineage['summary']}\n"
                f"历史建议: {knowledge['suggestion']}\n"
                f"执行动作: {action_type or '无'}\n"
                f"执行结果: {repair_result.get('message', '未执行')}"
            ),
        })
        return decision

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
