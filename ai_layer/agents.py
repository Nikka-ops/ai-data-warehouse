# -*- coding: utf-8 -*-
"""实时 AI 分析 Agent（Tool Calling 模式）"""
import os, sys
from langchain_openai import ChatOpenAI
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from ai_layer.tools import (
    query_data, query_knowledge, detect_realtime_anomaly,
    generate_insight, get_etl_status, get_forecast,
    get_proactive_insights, get_lambda_status, get_alert_investigations,
    ALL_TOOLS,
)

log = get_logger('agents')


def _get_llm():
    return ChatOpenAI(
        api_key=cfg.api_key, base_url=cfg.api_base_url,
        model=cfg.llm_model, temperature=cfg.agent_temperature,
        max_tokens=2000, timeout=90,
    )


def _make_executor(tools: list, system_msg: str, max_iter: int = 10) -> AgentExecutor:
    prompt = ChatPromptTemplate.from_messages([
        ('system', system_msg),
        ('human', '{input}'),
        ('placeholder', '{agent_scratchpad}'),
    ])
    agent = create_tool_calling_agent(_get_llm(), tools, prompt)
    return AgentExecutor(
        agent=agent, tools=tools, verbose=True,
        max_iterations=max_iter, return_intermediate_steps=True,
        handle_parsing_errors=True,
    )


# ══════════════════════════════════════════════════════════════
# Agent 1：实时异常检测
# ══════════════════════════════════════════════════════════════

ANOMALY_SYSTEM = """你是实时数据监控专家，负责检测流式数据中的异常并分析原因。

执行步骤：
1. 用 detect_realtime_anomaly 检测最近60分钟 order_cnt 异常
2. 用 detect_realtime_anomaly 检测最近60分钟 total_gmv 异常
3. 用 query_data 查当前最新5个分钟窗口：
   SELECT window_start, order_cnt, total_gmv, avg_price, top_category
   FROM dws.realtime_minute_stats ORDER BY window_start DESC LIMIT 5
4. 用 query_data 查最新告警：
   SELECT alert_time, severity, alert_type, detail
   FROM stream.ai_quality_alerts ORDER BY alert_time DESC LIMIT 5
5. 用 get_alert_investigations 查看 AI 已自动排查的告警结论
6. 用 get_forecast 查 order_cnt 预测趋势
7. 用 generate_insight 综合以上结果生成异常分析

输出：当前流量状态是否正常，已发现的异常，告警自动处置情况，预测走势。"""


def run_anomaly_agent():
    tools = [query_data, detect_realtime_anomaly, get_alert_investigations,
             get_forecast, generate_insight]
    log.info('启动实时异常检测 Agent')
    return _make_executor(tools, ANOMALY_SYSTEM, max_iter=10).invoke(
        {'input': '请对当前实时流数据进行全面异常检测，输出检测结论'}
    )


# ══════════════════════════════════════════════════════════════
# Agent 2：Lambda 架构数据一致性分析
# ══════════════════════════════════════════════════════════════

LAMBDA_SYSTEM = """你是 Lambda 架构数据工程师，负责验证批处理层（离线）和实时层（速度层）的数据一致性。

执行步骤：
1. 用 get_lambda_status 查看最近7天批实时对账状态
2. 用 query_data 查离线层批处理汇总：
   SELECT toStartOfMonth(stat_date) AS month, sum(order_cnt) AS orders,
          round(sum(total_gmv),0) AS gmv FROM dws.batch_daily_stats
   GROUP BY month ORDER BY month DESC LIMIT 3
3. 用 query_data 查今日实时层数据：
   SELECT count(DISTINCT order_id), round(sum(price),0)
   FROM ods.orders_stream WHERE event_time >= today()
4. 用 query_data 查服务层合并数据：
   SELECT source, sum(order_cnt) AS orders, round(sum(total_gmv),0) AS gmv
   FROM dws.serving_daily GROUP BY source ORDER BY source
5. 用 generate_insight 生成批实时一致性分析结论

输出：两层数据量、差异分析、Lambda 架构运行是否健康。"""


def run_lambda_agent():
    tools = [query_data, get_lambda_status, generate_insight]
    log.info('启动 Lambda 数据一致性分析 Agent')
    return _make_executor(tools, LAMBDA_SYSTEM, max_iter=8).invoke(
        {'input': '请分析 Lambda 架构批处理层和实时层的数据一致性'}
    )


# ══════════════════════════════════════════════════════════════
# Agent 3：自由分析（全工具权限）
# ══════════════════════════════════════════════════════════════

FREE_SYSTEM = """你是实时+离线数仓分析师，可自由调用工具分析数据。

可用实时表：
- ods.orders_stream / ods.payments_stream  实时流原始数据
- dwd.realtime_order_detail               Flink JOIN 宽表
- dws.realtime_minute_stats               分钟级聚合（Flink 窗口）
- dws.realtime_forecast                   AI 预测数据
- stream.ai_quality_alerts                AI 质检告警
- ads.realtime_hourly / realtime_category_today / realtime_state_today

可用离线+服务层表：
- ods.orders_batch                        历史批量数据（Lambda 离线层）
- dws.batch_daily_stats                   批处理日级汇总
- dws.serving_daily                       Lambda 服务层（批+实时合并）
- dws.serving_category                    品类维度服务层
- stream.lambda_reconciliation            批实时对账记录
- stream.alert_investigations             AI 告警排查记录
- stream.proactive_insights               AI 主动洞察

工作原则：先查数据 → 分析发现 → 生成洞察，用中文回答，结论有数字支撑。"""


def run_free_agent(user_goal: str):
    log.info('启动自由分析 Agent：%s', user_goal[:60])
    return _make_executor(ALL_TOOLS, FREE_SYSTEM, max_iter=12).invoke({'input': user_goal})


if __name__ == '__main__':
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else '1'
    if mode == '1':
        r = run_anomaly_agent()
    elif mode == '2':
        r = run_lambda_agent()
    else:
        r = run_free_agent('分析当前实时数据异常情况和批实时一致性状态')
    print('\n最终结论：', r['output'])
