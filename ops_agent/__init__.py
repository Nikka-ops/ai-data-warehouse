# -*- coding: utf-8 -*-
"""
链路运维 Agent

  probes.py      —— Flink/Kafka/Doris 健康探针（纯函数，可降级）
  inspector.py   —— 巡检 Agent：健康卡 + 异常研判（规则先判，命中才调 LLM）
  mcp_server.py  —— 把探针封装成 MCP 工具，供任意 MCP 客户端调用
  collect.py     —— 定时采集 Flink 指标落 stream.job_metrics，供看板/趋势用
"""
