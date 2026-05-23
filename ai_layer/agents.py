# -*- coding: utf-8 -*-
"""Kappa 架构 AI 分析 Multi-Agent（LangGraph Supervisor 模式）"""
import os
import sys
import operator
import json
from typing import TypedDict, Annotated
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import create_react_agent

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from ai_layer.tools import (
    query_data, query_knowledge, detect_realtime_anomaly,
    generate_insight, get_etl_status, get_forecast,
    get_proactive_insights, get_kappa_status, trigger_kappa_replay,
    get_remediation_status, get_alert_investigations,
)

log = get_logger('agents')

# ══════════════════════════════════════════════════════════════
# 状态定义
# ══════════════════════════════════════════════════════════════

class AgentState(TypedDict):
    messages: Annotated[list, operator.add]
    next_agent: str
    goal: str
    agent_outputs: Annotated[list, operator.add]
    iterations: int
    final_answer: str


# ══════════════════════════════════════════════════════════════
# 专家 Agent 工具分组
# ══════════════════════════════════════════════════════════════

DATA_TOOLS    = [query_data, get_etl_status]
ANOMALY_TOOLS = [detect_realtime_anomaly, query_data, get_remediation_status,
                 get_alert_investigations, get_forecast]
INSIGHT_TOOLS = [generate_insight, get_proactive_insights, query_knowledge, query_data]
KAPPA_TOOLS   = [get_kappa_status, trigger_kappa_replay, query_data, generate_insight]

AGENT_REGISTRY = {
    'DataAgent':    ('SQL 数据查询专家，负责从 ClickHouse 查询原始数据和 ETL 状态。',    DATA_TOOLS),
    'AnomalyAgent': ('实时异常检测专家，负责检测流数据异常、查看修复状态和预测趋势。',    ANOMALY_TOOLS),
    'InsightAgent': ('业务洞察专家，负责生成 AI 洞察、检索知识库和主动分析。',           INSIGHT_TOOLS),
    'KappaAgent':   ('Kappa 架构专家，负责监控流处理健康状态和历史数据回放。',           KAPPA_TOOLS),
}

SUPERVISOR_AGENTS = list(AGENT_REGISTRY.keys()) + ['FINISH']

SUPERVISOR_SYSTEM = """你是多 Agent 协调者，根据用户目标决定调用哪个专家 Agent。

专家 Agent 职责：
- DataAgent：SQL 查询 ClickHouse 数据，适合「查询数据、检查表」类任务
- AnomalyAgent：检测异常、查看告警和修复状态，适合「异常检测、监控分析」类任务
- InsightAgent：生成 AI 洞察、检索知识库，适合「分析原因、生成报告」类任务
- KappaAgent：Kappa 架构状态分析和历史回放，适合「流处理健康、Lag 分析」类任务
- FINISH：所有需要的信息已收集完毕，可以生成最终结论时选择

规则：
1. 每次只选一个 Agent 或 FINISH
2. 相同 Agent 连续调用不超过 2 次
3. 总轮次超过 8 次时强制选 FINISH
4. 以 JSON 格式回复：{"next": "AgentName", "reason": "理由"}"""


def _get_llm():
    return ChatOpenAI(
        api_key=cfg.api_key, base_url=cfg.api_base_url,
        model=cfg.llm_model, temperature=cfg.agent_temperature,
        max_tokens=2000, timeout=90,
    )


# ══════════════════════════════════════════════════════════════
# Supervisor 节点
# ══════════════════════════════════════════════════════════════

def supervisor_node(state: AgentState) -> AgentState:
    llm = _get_llm()
    context_parts = [f'用户目标：{state["goal"]}']
    if state['agent_outputs']:
        context_parts.append('已收集信息：')
        for item in state['agent_outputs'][-4:]:
            context_parts.append(f'- [{item["agent"]}] {str(item["output"])[:300]}')

    context_parts.append(f'当前迭代次数：{state["iterations"]}')

    messages = [
        SystemMessage(content=SUPERVISOR_SYSTEM),
        HumanMessage(content='\n'.join(context_parts)),
    ]

    if state['iterations'] >= 8:
        return {'next_agent': 'FINISH', 'iterations': state['iterations'] + 1, 'messages': []}

    resp = llm.invoke(messages)
    try:
        raw = resp.content.strip()
        if '```' in raw:
            raw = raw.split('```')[1].lstrip('json').strip()
        decision = json.loads(raw)
        next_agent = decision.get('next', 'FINISH')
    except Exception:
        next_agent = 'FINISH'

    if next_agent not in SUPERVISOR_AGENTS:
        next_agent = 'FINISH'

    log.info('Supervisor → %s (iter=%d)', next_agent, state['iterations'])
    return {'next_agent': next_agent, 'iterations': state['iterations'] + 1, 'messages': []}


def route_supervisor(state: AgentState) -> str:
    return state['next_agent']


# ══════════════════════════════════════════════════════════════
# 专家 Agent 节点工厂
# ══════════════════════════════════════════════════════════════

def _make_agent_node(agent_name: str, system_desc: str, tools: list):
    react_agent = create_react_agent(_get_llm(), tools)

    def node(state: AgentState) -> AgentState:
        log.info('%s 执行中...', agent_name)
        goal = state['goal']
        context = ''
        if state['agent_outputs']:
            prev = [f'[{o["agent"]}]: {str(o["output"])[:200]}' for o in state['agent_outputs'][-2:]]
            context = '\n已有信息：\n' + '\n'.join(prev)

        prompt = f'{system_desc}\n\n任务：{goal}{context}'
        result = react_agent.invoke({'messages': [HumanMessage(content=prompt)]})
        output_msg = result['messages'][-1]
        output_text = output_msg.content if hasattr(output_msg, 'content') else str(output_msg)

        return {
            'agent_outputs': [{'agent': agent_name, 'output': output_text}],
            'messages': [AIMessage(content=f'[{agent_name}] {output_text[:500]}')],
        }

    node.__name__ = agent_name.lower() + '_node'
    return node


# ══════════════════════════════════════════════════════════════
# 合成节点：生成最终报告
# ══════════════════════════════════════════════════════════════

def synthesize_node(state: AgentState) -> AgentState:
    llm = _get_llm()
    collected = '\n\n'.join(
        f'【{o["agent"]}】\n{o["output"]}' for o in state['agent_outputs']
    )
    prompt = f"""根据以下各专家 Agent 的分析结果，为用户目标生成综合结论报告。

用户目标：{state['goal']}

各 Agent 分析结果：
{collected}

要求：
1. 用中文回答，条理清晰
2. 关键数据要有数字支撑
3. 结论包括：当前状态、发现的问题、建议行动
4. 控制在 500 字以内"""

    resp = llm.invoke([HumanMessage(content=prompt)])
    final = resp.content.strip()
    log.info('综合分析完成，字数=%d', len(final))
    return {'final_answer': final, 'messages': [AIMessage(content=final)]}


# ══════════════════════════════════════════════════════════════
# 构建 Graph
# ══════════════════════════════════════════════════════════════

def _build_graph() -> StateGraph:
    g = StateGraph(AgentState)

    g.add_node('supervisor', supervisor_node)
    g.add_node('synthesize', synthesize_node)

    for name, (desc, tools) in AGENT_REGISTRY.items():
        g.add_node(name, _make_agent_node(name, desc, tools))
        g.add_edge(name, 'supervisor')

    g.add_conditional_edges(
        'supervisor',
        route_supervisor,
        {
            'DataAgent':    'DataAgent',
            'AnomalyAgent': 'AnomalyAgent',
            'InsightAgent': 'InsightAgent',
            'KappaAgent':   'KappaAgent',
            'FINISH':       'synthesize',
        },
    )
    g.add_edge('synthesize', END)
    g.set_entry_point('supervisor')
    return g.compile()


_GRAPH = None

def _get_graph():
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = _build_graph()
    return _GRAPH


def _run(goal: str, initial_agent: str | None = None) -> dict:
    init_state: AgentState = {
        'messages':     [HumanMessage(content=goal)],
        'next_agent':   initial_agent or '',
        'goal':         goal,
        'agent_outputs':[],
        'iterations':   0,
        'final_answer': '',
    }
    result = _get_graph().invoke(init_state)
    steps = [{'agent': o['agent'], 'output': o['output']} for o in result.get('agent_outputs', [])]
    return {'output': result.get('final_answer', ''), 'intermediate_steps': steps}


# ══════════════════════════════════════════════════════════════
# 公共接口（向后兼容）
# ══════════════════════════════════════════════════════════════

def run_anomaly_agent() -> dict:
    log.info('启动实时异常检测 Agent（LangGraph Supervisor）')
    return _run('请对当前实时流数据进行全面异常检测，输出检测结论、系统自动处置情况和预测趋势')


def run_kappa_agent() -> dict:
    log.info('启动 Kappa 架构状态分析 Agent（LangGraph Supervisor）')
    return _run('请分析 Kappa 架构流处理管道的健康状态、历史数据覆盖情况和 Kafka 消费 Lag')


def run_free_agent(user_goal: str) -> dict:
    log.info('启动自由分析 Agent：%s', user_goal[:60])
    return _run(user_goal)


if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else '1'
    if mode == '1':
        r = run_anomaly_agent()
    elif mode == '2':
        r = run_kappa_agent()
    else:
        r = run_free_agent('分析当前 Kappa 架构实时流处理状态和历史数据覆盖情况')
    print('\n最终结论：', r['output'])
