# -*- coding: utf-8 -*-
"""Kappa 架构 AI 分析 Agent（Tool Calling 模式）"""
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
    get_proactive_insights, get_kappa_status, trigger_kappa_replay,
    get_alert_investigations,
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
# Agent 2：Kappa 架构状态分析
# ══════════════════════════════════════════════════════════════

KAPPA_SYSTEM = """你是 Kappa 架构数据工程师，负责监控流处理管道健康状态和历史回放进度。

Kappa 架构原则：
- 单一处理路径：Kafka（可回放日志）→ Flink（统一流引擎）→ ClickHouse（服务层）
- 历史重算：通过 Flink 从 Kafka offset=earliest 重放，无需独立批处理管道
- 幂等写入：dws.kappa_hourly_agg 使用 ReplacingMergeTree，重放可安全多次执行

执行步骤：
1. 用 get_kappa_status 查看整体状态（Lag、历史覆盖、回放任务）
2. 用 query_data 查 Kappa 服务层当前覆盖：
   SELECT source, count() AS hours, sum(order_cnt) AS orders, round(sum(total_gmv),0) AS gmv
   FROM dws.kappa_serving_unified GROUP BY source
3. 用 query_data 查最近的消费 lag 趋势：
   SELECT toStartOfHour(check_time) AS hour, avg(lag) AS avg_lag, max(lag) AS max_lag
   FROM stream.kappa_consumer_lag WHERE check_time >= now() - INTERVAL 6 HOUR
   GROUP BY hour ORDER BY hour
4. 用 query_data 查今日实时处理量：
   SELECT sum(order_cnt) AS orders, round(sum(total_gmv),0) AS gmv
   FROM dws.realtime_minute_stats WHERE window_start >= today()
5. 若历史覆盖不足（最近30天内有空洞），判断是否需要触发回放
6. 用 generate_insight 生成 Kappa 架构健康分析报告

输出：流处理是否健康，历史数据覆盖情况，是否建议触发回放，当前处理 lag。"""


def run_kappa_agent():
    tools = [query_data, get_kappa_status, trigger_kappa_replay, generate_insight]
    log.info('启动 Kappa 架构状态分析 Agent')
    return _make_executor(tools, KAPPA_SYSTEM, max_iter=8).invoke(
        {'input': '请分析 Kappa 架构流处理管道的健康状态和历史数据覆盖情况'}
    )


# ══════════════════════════════════════════════════════════════
# Agent 3：自由分析（全工具权限）
# ══════════════════════════════════════════════════════════════

FREE_SYSTEM = """你是 Kappa 架构实时数仓分析师，可自由调用工具分析数据。

架构：Kafka（日志）→ Flink（统一流处理：实时 + 历史回放）→ ClickHouse（服务层）

可用实时表：
- ods.orders_stream / ods.payments_stream  实时流原始数据（Kafka 落地）
- dwd.realtime_order_detail               Flink JOIN 宽表
- dws.realtime_minute_stats               分钟级聚合（Flink 实时窗口）
- dws.realtime_forecast                   AI 预测数据
- stream.ai_quality_alerts                AI 质检告警（Flink 内嵌 AI 质量门控）
- ads.realtime_hourly / realtime_category_today / realtime_state_today

Kappa 历史聚合表（Flink 回放 Kafka 后写入）：
- dws.kappa_hourly_agg                    小时级聚合（回放结果，幂等）
- dws.kappa_serving_unified               统一服务视图（历史回放 + 实时互补）
- dws.kappa_daily_trend                   日级趋势视图
- dws.kappa_category_stats                品类维度视图
- ads.kappa_current_totals                当前 GMV 汇总

AI 与监控表：
- stream.kappa_replay_jobs                回放任务记录
- stream.kappa_consumer_lag               Kafka 消费 lag 监控
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
        r = run_kappa_agent()
    else:
        r = run_free_agent('分析当前 Kappa 架构实时流处理状态和历史数据覆盖情况')
    print('\n最终结论：', r['output'])
