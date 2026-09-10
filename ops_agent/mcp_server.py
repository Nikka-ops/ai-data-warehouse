# -*- coding: utf-8 -*-
"""
链路运维 MCP Server

把探针层的能力包装成标准 MCP 工具，让任何支持 MCP 的客户端
（Claude Desktop、Cursor、或自建 Agent）都能直接调用，
用一句自然语言就能查全链路健康，而不必逐个打开
Flink UI / Kafka UI / Doris 控制台。

为什么用 MCP 而不是直接把探针写成 LangChain @tool：
  * MCP 是模型无关、客户端无关的协议 —— 同一套工具，
    Claude Desktop 能用、Cursor 能用、自建 Agent 也能用，不锁定框架；
  * 工具的 schema、描述、参数校验由协议标准化，
    比每个框架各写一套 tool 定义更干净；
  * 运维工具天然适合做成独立进程对外暴露，MCP 的 stdio/SSE 传输正好。

启动（stdio 模式，供 MCP 客户端拉起）：
    python -m ops_agent.mcp_server

在 Claude Desktop / Cursor 的 MCP 配置里注册：
    {
      "mcpServers": {
        "realtime-dw-ops": {
          "command": "python",
          "args": ["-m", "ops_agent.mcp_server"],
          "cwd": "E:/ai-data-warehouse"
        }
      }
    }

依赖：mcp>=1.0（见 requirements.txt）。
"""

from __future__ import annotations

import json

from ops_agent import probes
from ops_agent import inspector

try:
    from mcp.server.fastmcp import FastMCP
except Exception as e:  # pragma: no cover
    raise SystemExit(
        '未安装 MCP SDK。请先执行：pip install "mcp>=1.0"\n'
        f'（探针与巡检逻辑不依赖 MCP，可直接用 python -m ops_agent.inspector）\n原始错误：{e}'
    )


mcp = FastMCP('realtime-dw-ops')


# ── 单组件探针 ────────────────────────────────────────────────

@mcp.tool()
def flink_health() -> str:
    """检查所有 Flink 作业的健康度：作业状态、checkpoint 失败/超时、背压。
    返回每个作业的状态与具体问题列表。"""
    return json.dumps(probes.probe_flink(), ensure_ascii=False, indent=2)


@mcp.tool()
def kafka_lag(consumer_groups: list[str] | None = None) -> str:
    """检查 Kafka 消费组的积压（Lag = 最新 offset - 已提交 offset）。
    不传 consumer_groups 则检查项目默认的几个消费组。
    Lag 持续增长通常意味着下游处理跟不上上游生产。"""
    return json.dumps(probes.probe_kafka(consumer_groups), ensure_ascii=False, indent=2)


@mcp.tool()
def doris_health() -> str:
    """检查 Doris 存储层健康度：SHOW BACKENDS 的 BE 存活情况、
    近期 Stream Load 导入失败率、动态分区探活。任一 BE 掉线即为严重故障。"""
    return json.dumps(probes.probe_doris(), ensure_ascii=False, indent=2)


# ── 全链路 ────────────────────────────────────────────────────

@mcp.tool()
def link_health_card() -> str:
    """一次性巡检整条实时链路（Flink + Kafka + Doris），返回结构化健康卡：
    整体状态（绿/黄/红）、各组件状态、以及需要关注的异常列表。
    这是「问一句看全链路」最常用的入口。"""
    snapshot = probes.probe_all()
    card = inspector.build_health_card(snapshot)
    return json.dumps(card, ensure_ascii=False, indent=2)


@mcp.tool()
def diagnose_link() -> str:
    """巡检全链路，并对发现的每个异常做根因研判（反压/倾斜/资源/checkpoint/
    外部依赖等方向）+ 处置建议。链路全绿时不做研判、直接返回健康卡。
    返回可读的巡检报告文本。"""
    card = inspector.inspect(persist=False)
    return inspector.format_card(card)


@mcp.tool()
def ask_link(question: str) -> str:
    """用自然语言询问链路状态，例如「现在链路健康吗」「消费有没有积压」
    「checkpoint 正常吗」。内部会先巡检再组织回答，像运维同事口头汇报那样说人话。"""
    return inspector.ask(question)


if __name__ == '__main__':
    mcp.run()
