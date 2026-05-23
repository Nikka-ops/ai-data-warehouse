# -*- coding: utf-8 -*-
"""自定义业务指标，通过 /metrics 暴露给 Prometheus"""
try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

# NL2SQL 指标
nl2sql_requests_total  = None
nl2sql_latency_seconds = None
nl2sql_repair_total    = None

# Agent 指标
agent_runs_total   = None
agent_success_rate = None

# 业务指标
active_alerts_gauge = None
kafka_lag_gauge     = None

def init_metrics():
    """初始化 Prometheus 指标"""
    if not HAS_PROMETHEUS:
        return
    global nl2sql_requests_total, nl2sql_latency_seconds, nl2sql_repair_total
    global agent_runs_total, agent_success_rate, active_alerts_gauge, kafka_lag_gauge

    nl2sql_requests_total  = Counter('nl2sql_requests_total',  'NL2SQL 请求总数', ['status'])
    nl2sql_latency_seconds = Histogram('nl2sql_latency_seconds', 'NL2SQL 延迟', buckets=[0.5, 1, 2, 5, 10])
    nl2sql_repair_total    = Counter('nl2sql_repair_total',    'SQL 修复次数')
    agent_runs_total       = Counter('agent_runs_total',       'Agent 运行总数', ['agent_type', 'status'])
    agent_success_rate     = Gauge('agent_success_rate',       'Agent 成功率')
    active_alerts_gauge    = Gauge('active_alerts_total',      '活跃告警数', ['severity'])
    kafka_lag_gauge        = Gauge('kafka_consumer_lag',       'Kafka 消费 Lag', ['group'])

def start_metrics_server(port: int = 9090):
    """启动 Prometheus metrics HTTP 服务"""
    if HAS_PROMETHEUS:
        init_metrics()
        start_http_server(port)
