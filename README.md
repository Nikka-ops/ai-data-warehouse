# AI Data Warehouse

> Kappa 架构实时数仓 + AI Agent 编排层：单一流引擎处理实时与历史数据，四类专家 Agent 覆盖查询、告警、知识检索与血缘分析，云原生就绪。

[![CI](https://github.com/Nikka-ops/ai-data-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Nikka-ops/ai-data-warehouse/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Flink](https://img.shields.io/badge/Apache_Flink-1.18-orange)](https://flink.apache.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow)](https://clickhouse.com)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-red)](https://kafka.apache.org)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.2-green)](https://langchain-ai.github.io/langgraph)
[![Feast](https://img.shields.io/badge/Feast-0.40-purple)](https://feast.dev)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

## 目录

- [架构概览](#架构概览)
- [核心设计原则](#核心设计原则)
- [分层说明](#分层说明)
  - [接入层](#1-接入层)
  - [流处理层](#2-流处理层kappa)
  - [存储层](#3-存储层)
  - [Feature Store](#4-feature-store)
  - [Agent 编排层](#5-agent-编排层)
  - [服务层](#6-服务层)
- [快速启动](#快速启动)
- [K8s 部署](#k8s-部署)
- [项目结构](#项目结构)
- [架构决策](#架构决策)

---

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────┐
│                           接入层                                     │
│  Kafka (消息队列) ← CDC/Debezium / 日志采集 / Mock 数据生成器         │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       流处理层 (Kappa)                               │
│  Flink Java 作业 — 事件时间处理、EXACTLY_ONCE、RocksDB 状态后端      │
│  DataStream API（特征计算）+ Table API（SQL 持续查询）                │
└─────────────────────────────────────────────────────────────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│   ClickHouse        │ │   Redis + ChromaDB  │ │   数据湖/Iceberg    │
│   (OLAP 分析查询)    │ │   (在线特征 + 向量)  │ │   (历史归档/回溯)    │
└─────────────────────┘ └─────────────────────┘ └─────────────────────┘
              │                     │                     │
              └─────────────────────┼─────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Feature Store (Feast)                           │
│           特征定义、版本管理、训练/推理一致性保证、物化调度              │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Agent 编排层 (LangGraph)                      │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐       │
│  │ SQL Agent  │ │ RAG Agent  │ │Alert Agent │ │ 血缘 Agent │       │
│  │ (NL2SQL+)  │ │ (知识召回)  │ │ (诊断+修复) │ │ (影响分析) │       │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘       │
│              Supervisor 节点动态路由，StateGraph 状态管理              │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        服务层                                        │
│  FastAPI (REST + gRPC stub) │ Streamlit Dashboard │ Grafana 监控    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 核心设计原则

**Kappa vs Lambda**：传统 Lambda 需维护两套管道（批 + 流），逻辑重复、一致性难保证。本项目用单一 Flink 引擎统一处理：

| 模式 | Kafka Offset | 输出表 | 场景 |
|---|---|---|---|
| 实时 | `latest` | `dws.realtime_minute_stats` | 分钟级在线监控 |
| 历史回放 | `earliest` | `dws.kappa_hourly_agg`（ReplacingMergeTree） | 历史补算、Schema 变更 |

**AI Agent 最小权限**：Supervisor LLM 动态路由，每个专家 Agent 只获得必要工具集，避免误操作。

**Self-RAG 闭环**：NL2SQL 生成后经 `EXPLAIN SYNTAX` 验证，失败则自动 repair；RAG 答案经 groundedness 评分，低置信度触发 conservative 重生成。

---

## 分层说明

### 1. 接入层

**`src/ingestion/`** — 数据进入系统的统一入口：

- **`producers/`**：抽象基类 `BaseProducer` + `KafkaProducer`（生产）+ `MockProducer`（测试用内存队列）+ `BrazilianEcommerceSimulator`（模拟巴西电商，10-30条/秒）
- **`cdc/`**：Debezium MySQL/PostgreSQL CDC 连接器配置生成器，通过 Kafka Connect REST API 注册
- **`schema/`**：Schema Registry 客户端 + Avro/Protobuf 序列化工具

```bash
# 启动模拟数据生成
python src/scripts/generate_mock_data.py
# 或指定速率
RATE_PER_SECOND=50 python src/scripts/generate_mock_data.py
```

---

### 2. 流处理层（Kappa）

**`src/streaming/`** — 生产级 Java Flink 作业：

| 作业 | 文件 | 说明 |
|---|---|---|
| 特征计算 | `FeatureComputeJob.java` | DataStream API，1分钟滚动窗口，双写 ClickHouse + Redis |
| 实时聚合 | `RealtimeAggregationJob.java` | Flink SQL（Table API），持续 TUMBLE 窗口查询 |

**关键配置**（`checkpoint_config.yaml`）：
- Checkpoint 间隔：60秒，EXACTLY_ONCE 语义
- 状态后端：RocksDB（增量检查点）
- 检查点目录：`s3://warehouse/flink-checkpoints`
- 重启策略：指数退避，最多 5 次

**UDF**：`GeoIpUdf`（IP → 巴西州）、`DeviceParserUdf`（UA 解析）、`TimeDecayUdf`（指数时间衰减）

```bash
# 提交 Flink 作业
bash scripts/run_flink_job.sh target/flink-jobs-1.0.0.jar

# 触发历史回放（Kappa replay）
bash scripts/trigger_backfill.sh 2024-01-01 2024-12-31

# Savepoint 管理
bash src/streaming/savepoint_manager.sh savepoint <job_id>
```

---

### 3. 存储层

三类存储各司其职：

#### ClickHouse（OLAP 分析）

`src/storage/clickhouse/` + `clickhouse/init/`（13个 SQL 文件按序执行）：

| 文件 | 层级 | 内容 |
|---|---|---|
| `01_init_tables.sql` | ODS | 基础表、数据库创建 |
| `02_kafka_stream.sql` | ODS | Kafka Engine + 物化视图 |
| `03_flink_realtime.sql` | DWD/DWS/ADS | Flink 输出 + 实时聚合视图 |
| `06_kappa_arch.sql` | DWS | Kappa 小时聚合、统一服务视图 |
| `12_business_monitor.sql` | 监控 | 业务告警、慢查询分析 |
| `13_alert_engine.sql` | 监控 | 告警引擎、Agent 决策日志 |

#### Redis（在线特征）

`src/storage/redis/feature_cache.py`：三级降级读取（Redis → ClickHouse → 默认值），key 格式 `feat:{entity_type}:{entity_id}`，批量 mget 使用 pipeline，p99 < 5ms。

#### Iceberg 数据湖（历史归档）

`src/storage/iceberg/` + `data_lake/`：基于 MinIO（S3 兼容）+ pyiceberg，定期将 ClickHouse 数据归档为 Iceberg 格式，支持时间点快照回溯查询。

```python
# 手动触发归档
from data_lake.lake_writer import LakeWriter
writer = LakeWriter(ch, adapter)
writer.run_daily_archive()
```

---

### 4. Feature Store

**`feature_store/feast_repo/`** — 基于 Feast 0.40 的特征管理：

| 组件 | 说明 |
|---|---|
| `entities.py` | 实体定义：user_id、seller_id、category |
| `feature_views.py` | FeatureView：user_stats（6特征）、seller_stats（5特征）、category_stats（5特征）|
| `feature_services.py` | 服务定义：recommendation_service、monitoring_service |
| `feast_store.py` | 主入口：在线查询、历史特征、物化调度、从 ClickHouse 同步 |

**训练/推理一致性**（PIT Correctness）：

```sql
-- ASOF JOIN 保证只取标签时间点之前的特征值，杜绝数据泄漏
SELECT l.entity_id, l.label, fv.feature_value
FROM labels l
ASOF LEFT JOIN feature_store.feature_values fv
  ON l.entity_id = fv.entity_id
 AND l.event_time >= fv.feature_time
```

**PSI 漂移监控**：每小时计算，阈值 0.10（监控）/ 0.25（告警）。

---

### 5. Agent 编排层

**`src/agent/`** — LangGraph Supervisor 多 Agent 架构：

```
用户目标
    │
    ▼
Supervisor（LLM JSON 路由）
    │
    ├──► SQLAgent     [query_data, get_etl_status]
    ├──► AnomalyAgent [detect_realtime_anomaly, get_forecast, get_remediation_status]
    ├──► InsightAgent [generate_insight, query_knowledge, get_proactive_insights]
    ├──► KappaAgent   [get_kappa_status, trigger_kappa_replay]
    │
    └──► synthesize（汇总所有 Agent 输出 → 最终报告）
```

**Self-RAG 闭环**（`ai_layer/nl2sql.py` / `ai_layer/rag_engine.py`）：

- NL2SQL：Generate → `EXPLAIN SYNTAX` 验证 → repair（最多 2 次）→ Execute → 置信度评分
- RAG：Retrieve → 批量相关性评分 → 低于 0.6 则改写查询重检索 → Generate → groundedness 评分

**告警 Agent**（`ai_layer/alert_engine/`）：LangGraph 13 节点图，并行诊断 → 安全门控 → 执行修复 → 验证 → 通知，支持钉钉/飞书/Slack。

---

### 6. 服务层

#### REST API（`src/api/rest/`）

| 路由 | 端点 | 说明 |
|---|---|---|
| `/api/v1/query/nl2sql` | POST | 自然语言 → SQL → 结果 |
| `/api/v1/query/sql` | POST | 直接执行 SELECT（安全拦截）|
| `/api/v1/alert/diagnose` | POST | 告警 AI 诊断 |
| `/api/v1/lineage/query` | POST | 血缘查询（upstream/downstream）|
| `/api/v1/lineage/impact/{table}` | GET | 影响分析 |
| `/api/v1/monitor/health` | GET | 服务健康检查 |
| `/api/v1/monitor/flink/jobs` | GET | Flink 作业状态 |
| `/api/v1/admin/schema/refresh` | POST | 刷新 NL2SQL Schema 缓存 |

**中间件**：JWT 鉴权（`ENABLE_AUTH=true` 开启）、令牌桶限流（60次/分/IP）、请求日志。

#### gRPC（`src/api/grpc/proto/`）

预留 `QueryService`（NL2SQL + 流式查询）和 `AgentService`（Agent 调用）proto 定义，生产环境可替换 REST。

#### 监控（`src/monitoring/`）

- Prometheus 指标：NL2SQL 延迟/成功率、Agent 调用次数、Kafka Lag、活跃告警数
- Grafana 看板：`flink_monitoring.json` / `clickhouse_monitoring.json` / `agent_performance.json`
- 通知渠道：`src/monitoring/alerts/notifiers/`（钉钉/飞书/Slack）

---

## 快速启动

### 前置要求

- Docker Engine 24+，内存 ≥ 8GB（推荐 16GB）
- DeepSeek API Key（[申请地址](https://platform.deepseek.com)）

### 一键启动

```bash
git clone https://github.com/Nikka-ops/ai-data-warehouse.git
cd ai-data-warehouse

cp .env.example .env
# 填入 DEEPSEEK_API_KEY=sk-xxxx

make start          # 等价于 docker compose up -d
make logs           # 查看实时日志
```

### Makefile 常用命令

```bash
make build          # 重新构建所有镜像
make start          # 启动所有服务
make stop           # 停止所有服务
make restart        # 重启
make logs           # 跟踪日志
make test           # 运行单元 + 集成测试
make lint           # ruff + mypy 代码检查
make clean          # 停止并清除 volumes
make benchmark      # NL2SQL 性能基准测试
make flink-job      # 提交 Flink 作业
make backfill       # 触发历史数据回填
```

### 服务地址

| 服务 | 地址 | 说明 |
|---|---|---|
| AI Dashboard | http://localhost/ai | Streamlit 智能查询 |
| API 文档 | http://localhost/api/docs | FastAPI Swagger |
| Superset BI | http://localhost | BI 看板（admin/admin）|
| Grafana | http://localhost/grafana | 系统监控 |
| Flink Web UI | http://localhost/flink | 流作业状态 |
| Kafka UI | http://localhost/kafka | 消息队列管理 |
| ClickHouse | http://localhost:8123/play | SQL 控制台 |

### 开发模式（不使用 Docker）

```bash
pip install -r requirements.txt -r requirements-dev.txt

# 单元测试
pytest tests/unit -v

# 集成测试（需要运行中的服务）
INTEGRATION_TEST=1 pytest tests/integration -v

# 性能基准
python src/scripts/benchmark.py
```

---

## K8s 部署

`k8s/` 目录包含完整 K8s 清单，支持生产集群部署：

```bash
# 一键部署到 K8s
make deploy-k8s

# 或逐步部署
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/clickhouse/
kubectl apply -f k8s/kafka/
kubectl apply -f k8s/flink/
kubectl apply -f k8s/agent/
kubectl apply -f k8s/monitoring/

# 查看 Pod 状态
make k8s-status
```

**K8s 组件说明**：

| 目录 | 资源 | 说明 |
|---|---|---|
| `k8s/flink/` | Deployment × 2 + ConfigMap | JobManager（1副本）+ TaskManager（2副本），RocksDB 检查点写 MinIO |
| `k8s/clickhouse/` | StatefulSet + PVC | 100Gi 持久化存储，密钥通过 Secret 注入 |
| `k8s/kafka/` | StatefulSet × 3 | 3副本 Kafka 集群，50Gi/节点，`RETENTION=-1` |
| `k8s/agent/` | Deployment + Ingress | 2副本 AI Agent 服务，Ingress 路由 `/api` |
| `k8s/monitoring/` | ConfigMap | Prometheus 抓取配置 + Grafana 数据源 |

---

## 项目结构

```
ai-data-warehouse/
├── Makefile                         # 常用命令封装
├── pom.xml                          # Flink Java 作业 Maven 构建
├── requirements.txt                 # Python 运行时依赖
├── requirements-dev.txt             # 测试/lint 依赖
├── docker-compose.yml               # 本地一键启动
├── .env.example                     # 环境变量模板
├── .pre-commit-config.yaml          # pre-commit hooks（ruff + 安全检查）
│
├── src/
│   ├── common/                      # 公共模块
│   │   ├── config.py                # Pydantic BaseSettings 配置
│   │   ├── models.py                # 核心数据模型（OrderEvent、AlertEvent 等）
│   │   └── utils.py                 # 日志、重试、工具函数
│   │
│   ├── ingestion/                   # 数据接入层
│   │   ├── producers/               # BaseProducer + KafkaProducer + MockProducer
│   │   ├── cdc/                     # Debezium MySQL/PostgreSQL CDC 配置
│   │   └── schema/                  # Schema Registry + Avro/Protobuf 序列化
│   │
│   ├── streaming/                   # 流处理层（Flink Java 作业）
│   │   ├── flink_jobs/
│   │   │   ├── common/              # FlinkEnvironment、ConfigConstants
│   │   │   ├── feature_compute/     # FeatureComputeJob（DataStream API）
│   │   │   └── realtime_agg/        # RealtimeAggregationJob（Table API + SQL）
│   │   ├── udf/                     # GeoIpUdf、DeviceParserUdf、TimeDecayUdf
│   │   ├── sql_jobs/                # Flink SQL DDL + 持续查询
│   │   └── savepoint_manager.sh     # Savepoint 管理脚本
│   │
│   ├── storage/                     # 存储层
│   │   ├── clickhouse/              # ClickHouse 客户端封装
│   │   ├── redis/                   # 特征缓存 + Lua 原子脚本
│   │   └── iceberg/                 # Iceberg Catalog 配置 + compaction
│   │
│   ├── agent/                       # AI Agent 编排层
│   │   ├── core/                    # BaseAgent、Supervisor、Memory
│   │   ├── tools/                   # ClickHouse/Kafka/Flink/血缘/通知工具
│   │   ├── agents/                  # SQLAgent、AlertAgent、RAGAgent、DiagnosticAgent
│   │   ├── rag/                     # VectorStore、Embedding、Retriever
│   │   ├── prompts/                 # YAML 提示词模板
│   │   └── agent_api.py             # Agent 快速调用接口
│   │
│   ├── lineage/                     # 数据血缘
│   │   ├── graph/                   # LineageGraph（NetworkX）、Node、Edge
│   │   ├── analyzer/                # ImpactAnalyzer、FreshnessTracker
│   │   └── openlineage_adapter.py   # OpenLineage 标准格式适配
│   │
│   ├── api/                         # 服务层
│   │   ├── rest/                    # FastAPI：路由、Schema、依赖注入、中间件
│   │   ├── grpc/                    # proto 定义 + gRPC 服务端存根
│   │   └── middleware/              # JWT 鉴权、限流、请求日志
│   │
│   ├── monitoring/                  # 可观测性
│   │   ├── metrics/                 # Prometheus 指标、业务指标采集
│   │   ├── dashboard/               # Streamlit App 入口
│   │   └── alerts/notifiers/        # 钉钉、飞书、Slack 通知
│   │
│   └── scripts/                     # 运维脚本
│       ├── benchmark.py             # NL2SQL P50/P95/Max 基准测试
│       └── generate_mock_data.py    # 巴西电商模拟数据生成
│
├── ai_layer/                        # AI 核心实现（Self-RAG + LangGraph）
│   ├── nl2sql.py                    # NL2SQL + EXPLAIN 验证 + repair 循环
│   ├── rag_engine.py                # Self-RAG + 批量相关性评分 + groundedness
│   ├── agents.py                    # LangGraph Supervisor 多 Agent
│   ├── alert_engine/                # 告警引擎（LangGraph 13节点图）
│   │   ├── orchestrator.py          # AlertOrchestrator（StateGraph）
│   │   ├── rule_engine.py           # 规则检测（GMV/Kafka Lag/回放失败等）
│   │   ├── aggregator.py            # 告警聚合去重（15分钟窗口）
│   │   ├── safety_gate.py           # 安全门控（critical 永远拒绝）
│   │   └── notifier.py              # 多渠道通知（飞书/钉钉/Slack/Generic）
│   └── lineage.py                   # SQL 血缘解析
│
├── feature_store/                   # Feast Feature Store
│   ├── feast_repo/                  # Feast 配置仓库
│   │   ├── feature_store.yaml       # Feast 项目配置（offline: file, online: Redis）
│   │   ├── entities.py              # 实体定义
│   │   ├── feature_views.py         # FeatureView 定义
│   │   └── feature_services.py      # FeatureService（recommendation/monitoring）
│   └── feast_store.py               # FeastStore 主入口（在线查询/历史特征/物化）
│
├── data_lake/                       # Iceberg 数据湖适配层
│   ├── iceberg_adapter.py           # pyiceberg REST Catalog 封装
│   └── lake_writer.py               # ClickHouse → Iceberg 定期归档
│
├── pipeline/
│   └── coordinator.py               # 轻量级管道编排器（6阶段 PipelineStage）
│
├── clickhouse/init/                 # ClickHouse 初始化 SQL（13个，按序执行）
├── k8s/                             # K8s 部署清单（Flink/ClickHouse/Kafka/Agent）
├── tests/
│   ├── unit/                        # 单元测试（SQL 安全、血缘图、告警去重、特征缓存）
│   ├── integration/                 # 集成测试（需 INTEGRATION_TEST=1）
│   ├── e2e/                         # 端到端测试（需 E2E_TEST=1）
│   └── fixtures/                    # 测试数据（sample_data.json、mock_lineage.json）
│
├── docs/
│   ├── architecture/
│   │   ├── architecture_overview.md
│   │   └── decision_records/        # ADR-001(Flink) ADR-002(ClickHouse) ADR-003(LangGraph)
│   └── deployment/
│       └── docker_compose_setup.md
│
└── .github/workflows/
    ├── ci.yml                       # lint(ruff+mypy) + test(pytest)
    └── cd.yml                       # Docker build + K8s deploy（tag 触发）
```

---

## 架构决策

完整决策记录见 `docs/architecture/decision_records/`，摘要如下：

| ADR | 决策 | 核心理由 |
|---|---|---|
| [ADR-001](docs/architecture/decision_records/adr-001-flink-vs-spark.md) | Flink vs Spark Streaming | 原生事件时间语义、RocksDB 大状态、毫秒级延迟 |
| [ADR-002](docs/architecture/decision_records/adr-002-clickhouse-vs-doris.md) | ClickHouse vs Doris | ReplacingMergeTree 幂等写入适配 Kappa 回放、Kafka Engine 直连 |
| [ADR-003](docs/architecture/decision_records/adr-003-agent-framework.md) | LangGraph vs LangChain ReAct | 并行节点、显式状态管理、条件路由重试循环 |

---

## 安全设计

- **密钥管理**：所有凭证通过 `.env` 注入，`.env` 已 gitignore，仓库只提交 `.env.example`
- **SQL 安全**：NL2SQL 硬拦截 `INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE`
- **DDL 限制**：NL2DDL 只允许 `CREATE VIEW`，视图名强制 `ads.*` 或 `dws.*` 前缀
- **Agent 安全门控**：`critical` 操作永远拒绝，`high` 限速 1次/小时，执行前 dry-run 验证
- **API 鉴权**：JWT 中间件（`ENABLE_AUTH=true` 开启），令牌桶限流 60次/分/IP
