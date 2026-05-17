# -*- coding: utf-8 -*-
"""三个实时数据分析 Agent（Tool Calling 模式）"""
import os, sys
from langchain_openai import ChatOpenAI
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from ai_layer.tools import (
    query_data, query_knowledge, detect_realtime_anomaly,
    generate_insight, save_report, ALL_TOOLS,
)

log = get_logger('agents')


def _get_llm():
    return ChatOpenAI(
        api_key=cfg.api_key,
        base_url=cfg.api_base_url,
        model=cfg.llm_model,
        temperature=cfg.agent_temperature,
        max_tokens=2000,
        timeout=90,
    )


def _make_executor(tools: list, system_msg: str, max_iter: int = 10) -> AgentExecutor:
    prompt = ChatPromptTemplate.from_messages([
        ('system', system_msg),
        ('human', '{input}'),
        ('placeholder', '{agent_scratchpad}'),
    ])
    agent = create_tool_calling_agent(_get_llm(), tools, prompt)
    return AgentExecutor(
        agent=agent, tools=tools,
        verbose=True, max_iterations=max_iter,
        return_intermediate_steps=True,
        handle_parsing_errors=True,
    )


# ══════════════════════════════════════════════════════════════
# Agent 1：实时异常检测
# ══════════════════════════════════════════════════════════════

ANOMALY_SYSTEM = """你是实时数据监控专家，负责检测流式数据中的异常并分析原因。

执行步骤：
1. 用 detect_realtime_anomaly 检测最近60分钟的 order_cnt 异常
2. 用 detect_realtime_anomaly 检测最近60分钟的 total_gmv 异常
3. 用 query_data 查询当前最新5个分钟窗口：
   SELECT window_start, order_cnt, total_gmv, avg_price, top_category
   FROM dws.realtime_minute_stats ORDER BY window_start DESC LIMIT 5
4. 用 query_data 查询最新告警：
   SELECT alert_time, severity, alert_type, detail, ai_suggestion
   FROM stream.ai_quality_alerts ORDER BY alert_time DESC LIMIT 10
5. 用 generate_insight 综合以上结果生成异常分析
6. 用 save_report 保存报告，标题"实时异常检测报告"

完成后给出结论：当前流量状态是否正常，主要风险点是什么。"""


def run_anomaly_agent():
    tools = [query_data, detect_realtime_anomaly, query_knowledge, generate_insight, save_report]
    log.info('启动实时异常检测 Agent')
    return _make_executor(tools, ANOMALY_SYSTEM, max_iter=10).invoke(
        {'input': '请对当前实时流数据进行全面异常检测，输出检测报告'}
    )


# ══════════════════════════════════════════════════════════════
# Agent 2：实时运营快报
# ══════════════════════════════════════════════════════════════

REPORT_SYSTEM = """你是实时运营数据分析师，负责生成当前时段的运营快报。

执行步骤：
1. 用 query_data 查今日小时趋势：
   SELECT * FROM ads.realtime_hourly ORDER BY hour_start DESC LIMIT 12
2. 用 query_data 查今日品类排行：
   SELECT * FROM ads.realtime_category_today LIMIT 10
3. 用 query_data 查今日各州排行：
   SELECT * FROM ads.realtime_state_today LIMIT 10
4. 用 query_data 查最近10分钟分钟统计：
   SELECT window_start, order_cnt, total_gmv, avg_price, top_category
   FROM dws.realtime_minute_stats ORDER BY window_start DESC LIMIT 10
5. 用 query_data 查最新告警（如有）：
   SELECT severity, detail FROM stream.ai_quality_alerts
   WHERE alert_time >= now() - INTERVAL 1 HOUR ORDER BY alert_time DESC LIMIT 5
6. 用 generate_insight 生成今日运营洞察
7. 用 save_report 保存报告，标题"实时运营快报"

输出今日运营核心数据摘要。"""


def run_report_agent():
    tools = [query_data, generate_insight, save_report]
    log.info('启动实时运营快报 Agent')
    return _make_executor(tools, REPORT_SYSTEM, max_iter=10).invoke(
        {'input': '请生成当前时段的实时运营快报'}
    )


# ══════════════════════════════════════════════════════════════
# Agent 3：自由分析（全工具权限）
# ══════════════════════════════════════════════════════════════

FREE_SYSTEM = """你是实时数据分析师，可自由调用工具分析实时流数据。

可用工具：query_data / query_knowledge / detect_realtime_anomaly / generate_insight / save_report

可用实时表：
- ods.orders_stream         原始订单流（price/state/product_category/order_status/event_time）
- ods.payments_stream       原始支付流（payment_type/payment_value/event_time）
- dwd.realtime_order_detail 订单+支付宽表（Flink JOIN）
- dws.realtime_minute_stats 分钟级聚合（Flink窗口：order_cnt/total_gmv/avg_price）
- stream.ai_quality_alerts  AI告警
- ads.realtime_hourly       今日小时视图（直接 SELECT）
- ads.realtime_category_today  今日品类视图
- ads.realtime_state_today     今日州排行视图

工作原则：先查数据 → 分析发现 → 生成洞察 → 保存报告，用中文回答，结论要有数字支撑。"""


def run_free_agent(user_goal: str):
    log.info('启动自由分析 Agent：%s', user_goal[:60])
    return _make_executor(ALL_TOOLS, FREE_SYSTEM, max_iter=12).invoke({'input': user_goal})


if __name__ == '__main__':
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else '3'
    if mode == '1':
        r = run_anomaly_agent()
    elif mode == '2':
        r = run_report_agent()
    else:
        r = run_free_agent('分析当前实时订单流量，找出异常，生成分析报告')
    print('\n最终结论：', r['output'])
