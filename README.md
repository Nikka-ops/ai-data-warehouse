# AI Data Warehouse

> Kappa 架构实时数仓 + AI 查询层：Kafka → Flink → ClickHouse 单一流引擎，NL2SQL / RAG 知识库 / LangGraph Agent 三层 AI 能力，本机可运行。

[![CI](https://github.com/Nikka-ops/ai-data-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Nikka-ops/ai-data-warehouse/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Flink](https://img.shields.io/badge/Apache_Flink-1.18-orange)](https://flink.apache.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow)](https://clickhouse.com)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-red)](https://kafka.apache.org)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2-green)](https://langchain-ai.github.io/langgraph)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

## 架构概览

```
┌─────────────────────────────────────────────────────────┐
│                       接入层                             │
│  kafka/producer.py — 模拟巴西电商订单，10-30 条/秒       │
│  src/ingestion/ — BaseProducer / KafkaProducer / Avro   │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                   流处理层（Kappa）                       │
│  flink/flink_stream_job.py                              │
│  事件时间窗口聚合 → 写入 ClickHouse ODS/DWD/DWS/ADS 层   │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                    存储层                                │
│  ClickHouse — ReplacingMergeTree / Kafka Engine         │
│  src/storage/clickhouse/client.py — 懒加载封装           │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                    AI 查询层                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │   NL2SQL    │  │  RAG 知识库  │  │  LangGraph      │ │
│  │ nl2sql.py   │  │rag_engine.py│  │  Agent 编排      │ │
│  │ Self-RAG    │  │ ChromaDB    │  │  agents.py       │ │
│  │ + repair    │  │ + 相关性评分 │  │  Supervisor 路由  │ │
│  └─────────────┘  └─────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                  服务层（FastAPI）                        │
│  POST /api/v1/query/nl2sql — 自然语言查询                │
│  POST /api/v1/query/sql    — 直接执行 SELECT             │
│  GET  /health                                           │
└─────────────────────────────────────────────────────────┘
```

---

## 核心功能

### NL2SQL（Self-RAG 闭环）

`ai_layer/nl2sql.py` 实现四阶段流程：

1. **Generate** — LLM 根据 Schema + 历史会话生成 SQL
2. **Validate** — `EXPLAIN SYNTAX` 校验语法；`utils/sql_validator.py` 拦截 DDL/DML
3. **Repair** — 失败则附带错误信息重新生成，最多 2 次
4. **Insight** — 对结果集调用 LLM 生成业务洞察 + 置信度评分

### RAG 知识库

`ai_layer/rag_engine.py` 基于 ChromaDB + OpenAI Embeddings：

- 文档入库：PDF/Markdown 切片 → 向量化 → 存储
- 批量相关性评分，低于阈值则改写查询重检索
- Groundedness 评分，低置信度触发 conservative 重生成

### LangGraph Agent

`ai_layer/agents.py` + `src/agent/` 实现 Supervisor 多 Agent 架构：

- **SQLAgent** — 调用 NL2SQL 工具查询 ClickHouse
- **RAGAgent** — 调用 RAG 工具检索知识库
- **Supervisor** — LLM JSON 路由，动态分发到合适的 Agent

---

## 快速启动

### 前置要求

- Docker Engine 24+ 且内存 ≥ 8 GB
- DeepSeek 或 OpenAI API Key

### Docker 启动

```bash
git clone https://github.com/Nikka-ops/ai-data-warehouse.git
cd ai-data-warehouse

cp .env.example .env
# 编辑 .env，填入 DEEPSEEK_API_KEY=sk-xxxx

docker compose up -d
```

服务地址：

| 服务 | 地址 |
|---|---|
| API 文档 | http://localhost:8000/docs |
| ClickHouse | http://localhost:8123/play |
| Kafka UI | http://localhost:8080 |

### 本机开发（不用 Docker）

```bash
pip install -r requirements.txt -r requirements-dev.txt
pip install -e . --no-deps

# 运行单元测试
pytest tests/unit -v

# 启动 API 服务（需要 ClickHouse 在线）
python -m src.api.rest.main

# 模拟数据写入 Kafka
python kafka/producer.py

# 启动 Flink 流处理（纯 Python，无需 Java）
python flink/flink_stream_job.py
```

### 快速查询示例

```python
from ai_layer.nl2sql import nl2sql

result = nl2sql("今天的 GMV 是多少？")
print(result["sql"])
print(result["data"])
print(result["insight"])
```

```bash
# 通过 REST API 查询
curl -X POST http://localhost:8000/api/v1/query/nl2sql \
  -H "Content-Type: application/json" \
  -d '{"question": "最近 1 小时订单量排名前 5 的品类"}'
```

---

## 项目结构

```
ai-data-warehouse/
├── config.py                    # 单一配置源（pydantic-settings + dataclass 降级）
├── kafka/
│   └── producer.py              # Kafka 数据生产者（模拟巴西电商订单）
├── flink/
│   └── flink_stream_job.py      # Flink 流处理（事件时间窗口 → ClickHouse）
│
├── ai_layer/                    # AI 核心实现
│   ├── nl2sql.py                # NL2SQL + EXPLAIN 验证 + repair 循环
│   ├── rag_engine.py            # RAG 知识库（ChromaDB + Self-RAG）
│   ├── tools.py                 # LangChain tools（ClickHouse 查询）
│   └── agents.py                # LangGraph Supervisor 多 Agent
│
├── src/
│   ├── agent/                   # Agent 框架
│   │   ├── core/                # BaseAgent、Memory、Supervisor
│   │   ├── agents/              # SQLAgent、RAGAgent
│   │   ├── tools/               # clickhouse_tool、kafka_tool、flink_tool
│   │   └── agent_api.py         # query() / rag_query() 快速调用接口
│   │
│   ├── api/rest/                # FastAPI 服务层
│   │   ├── main.py              # 应用入口，挂载 /query 路由
│   │   ├── schemas.py           # QueryRequest / QueryResponse
│   │   ├── dependencies.py      # ClickHouse 客户端依赖注入
│   │   └── routers/query.py     # /nl2sql、/sql 路由
│   │
│   ├── common/                  # 公共模块
│   │   ├── config.py            # config.py 的薄重导出
│   │   ├── models.py            # 核心数据模型
│   │   └── utils.py             # get_logger()、retry_with_backoff()
│   │
│   ├── ingestion/               # 数据接入层
│   │   ├── producers/           # BaseProducer、KafkaProducer、MockProducer
│   │   └── schema/              # Schema Registry 客户端 + Avro 序列化
│   │
│   └── storage/clickhouse/      # ClickHouse 客户端
│       └── client.py            # get_client()、ClickHouseClient（懒加载）
│
├── utils/                       # 共享工具
│   ├── ch_client.py             # get_ch_client() 工厂（带 ch_retry）
│   ├── sql_validator.py         # validate_sql() / check_sql() 安全校验
│   ├── retry.py                 # @ch_retry、@llm_retry（tenacity）
│   └── logger.py                # get_logger() 薄重导出
│
├── tests/
│   ├── unit/                    # 61 个单元测试，全部离线（MagicMock）
│   ├── integration/             # 集成测试（需 INTEGRATION_TEST=1）
│   └── e2e/                     # 端到端测试（需 E2E_TEST=1）
│
├── clickhouse/init/             # ClickHouse 初始化 SQL（按序执行）
├── docker-compose.yml           # 本地一键启动
├── pyproject.toml               # 构建配置 + pytest pythonpath
├── requirements.txt
└── requirements-dev.txt
```

---

## 安全设计

- **SQL 安全**：`utils/sql_validator.py` 硬拦截 `INSERT / UPDATE / DELETE / DROP / CREATE / ALTER / TRUNCATE`，关键字边界匹配，标识符中的关键字（如 `created_at`）不触发
- **密钥管理**：所有凭证通过 `.env` 注入，`.env` 已 gitignore，仓库只提交 `.env.example`
- **Agent 最小权限**：每个 Agent 只获得必要工具集，SQLAgent 只能执行 SELECT

---

## 数据流与表结构

```
Kafka Topic: orders
    │  (Avro / JSON)
    ▼
ClickHouse Kafka Engine (ODS)
    │  物化视图
    ▼
dwd.realtime_order_detail        -- 明细层（Flink 写入）
    │
    ├──► dws.realtime_minute_stats   -- 分钟聚合（GMV、订单量、UV）
    └──► ads.realtime_hourly         -- 小时汇总（对外服务视图）
```

---

## 配置说明

所有配置集中在根目录 `config.py`，通过环境变量或 `.env` 文件注入：

| 变量 | 说明 | 默认值 |
|---|---|---|
| `CH_HOST` | ClickHouse 主机 | `localhost` |
| `CH_PORT` | ClickHouse 端口 | `9000` |
| `CH_USER` | ClickHouse 用户名 | `default` |
| `CH_PASSWORD` | ClickHouse 密码 | `""` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka 地址 | `localhost:9092` |
| `DEEPSEEK_API_KEY` | LLM API Key | — |
| `DEEPSEEK_BASE_URL` | LLM API 地址 | DeepSeek 官方 |
| `KNOWLEDGE_BASE_PATH` | RAG 文档目录 | `./knowledge_base` |
