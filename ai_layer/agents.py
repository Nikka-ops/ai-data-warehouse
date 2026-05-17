# -*- coding: utf-8 -*-
"""
三个专用 Agent - Tool Calling 模式
工具定义统一来自 ai_layer/tools.py
"""
import os, sys
from langchain_openai import ChatOpenAI
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from ai_layer.tools import (
    query_data, query_knowledge, calculate_anomalies,
    generate_insight, save_report, ALL_TOOLS,
)

log = get_logger('agents')


def get_llm(temperature: float | None = None):
    return ChatOpenAI(
        api_key=cfg.api_key,
        base_url=cfg.api_base_url,
        model=cfg.llm_model,
        temperature=temperature if temperature is not None else cfg.agent_temperature,
        max_tokens=2000,
        timeout=90,
    )


def _make_executor(tools: list, system_msg: str, max_iter: int = 10) -> AgentExecutor:
    llm = get_llm()
    prompt = ChatPromptTemplate.from_messages([
        ('system', system_msg),
        ('human', '{input}'),
        ('placeholder', '{agent_scratchpad}'),
    ])
    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        max_iterations=max_iter,
        return_intermediate_steps=True,
        handle_parsing_errors=True,
    )


# ══════════════════════════════════════════════════════════════
# Agent 1：销售异常分析（支持历史 + 实时异常检测）
# ══════════════════════════════════════════════════════════════

ANOMALY_SYSTEM = """你是一位专业的数据分析师，擅长销售异常检测。

历史数据异常分析步骤：
1. 用 calculate_anomalies 检测 dws.order_daily 表 gmv 字段的异常，where条件 dt >= '2017-01-01'
2. 用 query_data 查询异常峰值日期前后14天每日数据
3. 用 query_knowledge 查询"巴西电商节日规律"
4. 用 generate_insight 生成异常原因分析
5. 用 save_report 保存完整报告，标题"销售异常分析报告"

实时数据异常分析步骤（如果用户要求实时）：
1. 用 query_data 查询最近30分钟的分钟统计：SELECT * FROM dws.realtime_minute_stats WHERE window_start >= now() - INTERVAL 30 MINUTE ORDER BY window_start
2. 用 calculate_anomalies 检测 dws.realtime_minute_stats 的 total_gmv 字段异常
3. 用 query_data 查询 stream.ai_quality_alerts 中的最新告警
4. 用 generate_insight 分析实时异常原因
5. 用 save_report 保存报告

完成所有步骤后给出最终结论。"""

def run_anomaly_agent(realtime: bool = False):
    tools = [query_data, calculate_anomalies, query_knowledge, generate_insight, save_report]
    executor = _make_executor(tools, ANOMALY_SYSTEM, max_iter=10)
    mode = '实时' if realtime else '历史'
    log.info('启动异常分析 Agent（%s模式）', mode)
    result = executor.invoke({'input': f'请对{"实时流" if realtime else "历史"}销售数据进行异常检测分析'})
    return result


# ══════════════════════════════════════════════════════════════
# Agent 2：自动周报（含实时快照）
# ══════════════════════════════════════════════════════════════

WEEKLY_SYSTEM = """你是一位数据分析师，负责生成每周数据报告，报告同时包含历史趋势和实时快照。

按以下步骤生成完整周报：
1. 用 query_data 查月度KPI（历史趋势）：
   SELECT ym, round(gmv,0) AS GMV, order_cnt AS 订单数, user_cnt AS 用户数, round(mom_gmv_rate,2) AS 环比增长率 FROM ads.monthly_kpi ORDER BY ym DESC LIMIT 6
2. 用 query_data 查品类Top10：
   SELECT product_category AS 品类, round(sum(gmv),0) AS 总GMV, sum(order_cnt) AS 订单数 FROM dws.category_daily GROUP BY product_category ORDER BY 总GMV DESC LIMIT 10
3. 用 query_data 查最新月省份排行：
   SELECT state AS 州, round(gmv,0) AS GMV, rank_by_gmv AS 排名 FROM ads.state_sales_rank WHERE dt_month=(SELECT max(dt_month) FROM ads.state_sales_rank) ORDER BY rank_by_gmv LIMIT 10
4. 用 query_data 查今日实时快照：
   SELECT count(*) AS 今日订单数, round(sum(price),0) AS 今日GMV, round(avg(price),2) AS 均价 FROM ods.orders_stream WHERE event_time >= today()
5. 用 generate_insight 对GMV趋势和品类数据生成洞察
6. 用 save_report 整合为Markdown周报保存，标题"数据分析周报"

最终输出周报摘要。"""

def run_weekly_report_agent():
    tools = [query_data, generate_insight, save_report]
    executor = _make_executor(tools, WEEKLY_SYSTEM, max_iter=10)
    log.info('启动自动周报 Agent')
    return executor.invoke({'input': '请生成本周数据分析周报（含历史趋势和今日实时快照）'})


# ══════════════════════════════════════════════════════════════
# Agent 3：自由分析
# ══════════════════════════════════════════════════════════════

FREE_SYSTEM = """你是一位专业的数据分析师，可分析历史数据和实时流数据。

可用工具：query_data / query_knowledge / calculate_anomalies / generate_insight / save_report

可用数据表：
历史数据（2016-2018）：
- ads.monthly_kpi：ym、gmv、order_cnt、user_cnt、mom_gmv_rate
- dws.category_daily：dt、product_category、gmv、order_cnt
- dws.order_daily：dt、gmv、order_cnt、user_cnt、avg_order_value
- ads.state_sales_rank：dt_month、state、gmv、order_cnt、rank_by_gmv
- dwd.order_detail：order_date、state、product_category、price、order_status、delivery_days

实时数据（近24小时）：
- ods.orders_stream：order_id、customer_id、product_category、price、state、order_status、event_time
- dwd.realtime_order_detail：product_category、state、price、payment_type、order_status、event_date
- dws.realtime_minute_stats：window_start、window_end、order_cnt、total_gmv、avg_price
- stream.ai_quality_alerts：alert_time、alert_type、severity、detail、ai_suggestion

原则：先查数据 → 再生成洞察 → 最后保存报告，中文回答，结论要有数字支撑。"""

def run_free_agent(user_goal: str):
    executor = _make_executor(ALL_TOOLS, FREE_SYSTEM, max_iter=12)
    log.info('启动自由分析 Agent：%s', user_goal[:50])
    return executor.invoke({'input': user_goal})


if __name__ == '__main__':
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else '3'
    if mode == '1':
        result = run_anomaly_agent()
    elif mode == '1r':
        result = run_anomaly_agent(realtime=True)
    elif mode == '2':
        result = run_weekly_report_agent()
    else:
        result = run_free_agent('分析今日实时销售情况，对比历史同期，给出洞察报告')
    print('\n最终结论：', result['output'])
