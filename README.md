# AI 数仓系统（Kappa 架构）

> 基于 Kappa 架构的 AI 原生数据仓库：一套流引擎同时处理实时数据与历史回算，AI 贯穿数据处理、质量管控、特征工程、用户交互全链路。

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Flink](https://img.shields.io/badge/Apache_Flink-1.18-orange)](https://flink.apache.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow)](https://clickhouse.com)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-red)](https://kafka.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7.2-red)](https://redis.io)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-teal)](https://fastapi.tiangolo.com)
[![LangChain](https://img.shields.io/badge/LangChain-0.3.25-green)](https://langchain.com)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

## 目录

- [核心理念](#核心理念)
- [整体架构](#整体架构)
- [模块详解](#模块详解)
  - [Kappa 流处理层](#1-kappa-流处理层)
  - [ClickHouse 数据分层](#2-clickhouse-数据分层)
  - [Feature Store](#3-feature-store)
  - [AI 能力层](#4-ai-能力层)
  - [告警自动处置](#5-告警自动处置闭环)
  - [FastAPI 特征服务](#6-fastapi-特征服务)
  - [前端与可视化](#7-前端与可视化)
- [快速启动](#快速启动)
- [服务地址](#服务地址)
- [项目结构](#项目结构)
- [技术亮点](#技术亮点)

---

## 核心理念

**传统 Lambda 架构**需要维护两套独立管道（批处理 + 流处理），代码重复、一致性难以保证。

**本项目采用 Kappa 架构**，核心思路：

- **Kafka 是永久日志**：`RETENTION=-1`，消息永不过期，可以随时从任意时间点回放
- **Flink 是唯一引擎**：同一套代码，通过 `--replay` 标志切换实时模式和历史回算模式
- **ClickHouse 是统一 Serving 层**：`ReplacingMergeTree` 保证回放幂等，历史数据和实时数据通过同一张视图对外服务
- **AI 贯穿全链路**：从 ETL 质量管控、异常根因分析、告警自动处置，到自然语言查询、特征自动生成

---

## 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│                      用户层（前端）                            │
│  Streamlit AI 对话  │  Apache Superset BI  │  Grafana 监控    │
│                    ↑  Nginx :80 统一入口  ↑                   │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│                       AI 能力层                               │
│  NL2SQL  │  RAG 知识库  │  Agent(13工具)  │  预测  │  自动处置 │
└──────────────────────────────────────────────────────────────┘
┌────────────────────────┐  ┌─────────────────────────────────┐
│     Feature Store      │  │      FastAPI 特征服务 :8000      │
│  离线层：ClickHouse     │  │  /api/features/*  9个端点        │
│  在线层：Redis          │  │  在线查询 p99 < 20ms             │
│  PIT 训练集构建         │  └─────────────────────────────────┘
│  PSI 漂移监控           │
│  LLM 自动特征工程       │
└────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│                  数据仓库层（ClickHouse）                      │
│  ODS → DWD → DWS → ADS（11个SQL文件，全视图服务）             │
│  feature_store.*（7表+4视图）  ml_metadata.*（3表+2视图）      │
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│                  Kappa 流处理层（Flink）                       │
│  实时模式：offset=latest  →  分钟级聚合  →  dws.realtime_*    │
│  回放模式：offset=earliest →  小时级聚合 →  dws.kappa_*（幂等）│
└──────────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────────┐
│                   数据接入层（Kafka）                          │
│  orders_stream │ payments_stream │ flink.minute_stats        │
│  flink.realtime_dwd │ flink.alerts                          │
│  RETENTION=-1（永久保留，支持任意时间点回放）                   │
└──────────────────────────────────────────────────────────────┘
```

---

## 模块详解

### 1. Kappa 流处理层

`flink/flink_stream_job.py` 同时支持两种运行模式：

**实时模式**（默认）
```bash
python flink/flink_stream_job.py --mode python
```
- 消费 Kafka 最新消息（`auto_offset_reset=latest`）
- 每1分钟滚动窗口聚合 → 写入 `dws.realtime_minute_stats`
- 内嵌 AI 质量门控：规则引擎毫秒级检测，触发时调用 LLM 分析 → 写入 `stream.ai_quality_alerts`

**回放模式**（历史重算）
```bash
python flink/flink_stream_job.py --replay --replay-from "2024-01-01"
```
- 从 Kafka `offset=earliest` 开始消费
- 每小时窗口聚合 → 写入 `dws.kappa_hourly_agg`（`ReplacingMergeTree` 幂等）
- 写入回放任务记录 `stream.kappa_replay_jobs`，可随时查看进度

**统一服务视图**

`kappa_serving_unified` 将历史回放数据与实时增量 UNION ALL，查询层无需关心数据来源：

```sql
-- 实时数据和历史回放数据，对查询层透明
SELECT * FROM dws.kappa_serving_unified
WHERE hour_start >= today() - 30
```

---

### 2. ClickHouse 数据分层

11 个初始化 SQL 文件，按启动顺序执行：

| 文件 | 层级 | 内容 |
|------|------|------|
| `01_init_tables.sql` | ODS | 基础表结构、数据库创建 |
| `02_kafka_stream.sql` | ODS | Kafka Engine 表 + 物化视图，实时订阅消息 |
| `03_flink_realtime.sql` | DWD/DWS/ADS | Flink 输出表 + 实时聚合视图 |
| `04_ai_etl.sql` | 监控 | AI ETL 质量评分、审计日志 |
| `05_ai_analytics.sql` | ADS | 主动洞察、时序预测结果存储 |
| `06_kappa_arch.sql` | DWS | Kappa 小时聚合、统一服务视图、回放任务记录 |
| `07_alert_investigation.sql` | 监控 | AI 告警排查历史记录 |
| `08_serving_layer.sql` | ADS | Kappa 日趋势、品类统计服务视图 |
| `09_remediation.sql` | 监控 | 系统告警、自动处置审计表、处置看板视图 |
| `10_feature_store.sql` | Feature Store | 7张表 + 4个视图，完整特征存储体系 |
| `11_ml_metadata.sql` | ML Metadata | 实验追踪、模型血缘、预测日志 |

---

### 3. Feature Store

解决 **Training-Serving Skew**（训练与服务数据不一致）问题的核心模块。

#### 架构

```
features/*.yaml（特征声明）
        │
        ▼
  registry.py ──► feature_store.feature_definitions（ClickHouse 注册表）
        │
        ▼
  pipeline.py（每5分钟）
        │
        ├──► offline_store.py ──► feature_store.feature_values（ClickHouse，TTL 90天）
        │                         INSERT INTO...SELECT，零 Python 数据中转
        │
        └──► online_store.py  ──► Redis（KV，按特征 TTL 设置）
                                  三级降级：Redis → ClickHouse 离线 → 契约默认值
```

#### 已注册特征（17个）

| 特征组 | 实体键 | 特征 |
|--------|--------|------|
| `user_behavior` | `customer_id` | order_count_7d、gmv_7d、cancel_rate_30d、avg_order_price_30d、days_since_last_order、order_count_1h |
| `category_stats` | `product_category` | order_volume_1h、avg_price_7d、cancel_rate_7d、gmv_share_today |
| `seller_stats` | `seller_id` | gmv_7d、order_count_7d、cancel_rate_30d、unique_buyers_30d |
| `temporal` | `global` | hour_of_day、is_peak_hour、is_weekend |

#### PIT 正确性（`pit_join.py`）

构建训练集时使用 ClickHouse `ASOF JOIN`，确保样本只能使用**标签时间点之前**的特征值，彻底防止未来数据泄漏：

```sql
SELECT l.entity_id, l.label, fv.feature_value
FROM labels l
ASOF LEFT JOIN feature_store.feature_values fv
ON l.entity_id = fv.entity_id
AND l.event_time >= fv.feature_time   -- 只取 feature_time ≤ label_time 的最新值
```

#### 特征漂移监控（`drift_monitor.py`）

每小时计算 PSI（Population Stability Index）：

| PSI 区间 | 状态 |
|----------|------|
| < 0.10 | 🟢 稳定 |
| 0.10 ~ 0.25 | 🟡 监控 |
| > 0.25 | 🔴 漂移告警 |

#### 自动特征工程（`auto_feature.py`）

调用 DeepSeek LLM 分析数据 Schema 和现有特征，生成新特征计算 SQL，经 ClickHouse `EXPLAIN` 验证后保存为 YAML，人工审核后激活。

---

### 4. AI 能力层

`ai_layer/` 目录下 8 个模块，13 个 Agent 工具：

| 模块 | 功能 |
|------|------|
| `nl2sql.py` | 自然语言 → ClickHouse SQL（禁止 DDL/DML，Schema TTL 缓存 5min）|
| `nl2ddl.py` | 自然语言 → CREATE VIEW（限 `ads.*`、`dws.*` 命名空间）|
| `rag_engine.py` | ChromaDB + SentenceTransformers，回答指标定义、字段含义 |
| `agents.py` | LangChain ReAct Agent，多步自主推理 |
| `tools.py` | 13 个工具的唯一定义来源 |
| `insight_engine.py` | 定时主动洞察（异常、趋势、风险），写入 `stream.proactive_insights` |
| `forecaster.py` | Holt 双指数平滑时序预测 |
| `alert_investigator.py` | 告警自动处置闭环（见下节）|

**13 个 Agent 工具**

| 工具 | 说明 |
|------|------|
| `query_data` | 执行 ClickHouse SELECT |
| `query_knowledge` | RAG 知识库问答 |
| `detect_realtime_anomaly` | 2σ 基线法异常检测 |
| `generate_insight` | LLM 生成业务洞察 |
| `get_etl_status` | ETL 质量评分查询 |
| `get_forecast` | 时序预测 |
| `get_proactive_insights` | 主动洞察列表 |
| `get_kappa_status` | Kappa 回放任务 + 消费 Lag 状态 |
| `trigger_kappa_replay` | 触发历史数据回放 |
| `get_remediation_status` | 告警处置状态查询 |
| `get_alert_investigations` | 告警排查记录 |
| `get_feature_status` | 特征组注册状态 + 漂移情况 |
| `query_feature_values` | 查询实体特征值（离线层）|

---

### 5. 告警自动处置闭环

`ai_layer/alert_investigator.py` 每 30 秒轮询，全自动完成检测→分析→执行→反馈：

```
多源检测（30秒轮询）
  ├── 数据质量告警（stream.ai_quality_alerts）
  ├── Kappa 回放任务失败
  ├── Kafka 消费 Lag（HIGH > 50k，CRITICAL > 200k）
  └── ETL 质量评分劣化（< 70分）
          │ 按严重度排序：CRITICAL > HIGH > MEDIUM > LOW
          ▼
  LLM 根因分析（DeepSeek）
  → 输出：root_cause + action_type + confidence
          │
          ▼ 执行 Playbook
  ├── RESTART_REPLAY    → 写入回放任务 + subprocess 启动 Flink
  ├── TRIGGER_ETL       → subprocess 触发 ETL 修复
  ├── QUARANTINE_WINDOW → 隔离异常时间窗口，写入系统告警
  └── SCALE_CONSUMER    → 输出消费者扩容建议
          │
          ▼
  审计写入（stream.remediation_actions）
  结构化日志反馈（✅ 成功 / 👁 监控 / 🚨 失败）
```

---

### 6. FastAPI 特征服务

`api/feature_api.py`，Nginx 通过 `/api/` 路由转发：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/features/groups` | 列出所有特征组 |
| GET | `/features/definitions/{group}` | 特征组详情 |
| GET | `/features/online/{group}/{entity_id}` | 单实体在线查询（Redis 优先）|
| POST | `/features/online/batch` | 批量查询（Redis pipeline，p99 < 20ms）|
| POST | `/features/dataset/build` | 触发 PIT 训练集构建（后台任务）|
| GET | `/features/dataset/list` | 历史训练集列表 |
| POST | `/features/suggest` | LLM 自动特征建议 |
| GET | `/features/drift/{group}` | 特征漂移状态 |

---

### 7. 前端与可视化

所有服务通过 Nginx 统一从 `:80` 对外暴露：

| 路径 | 服务 | 说明 |
|------|------|------|
| `/` | Superset | BI 看板，35+ 数据集自动注册 |
| `/ai` | Streamlit | AI 对话界面（WebSocket）|
| `/api/` | FastAPI | Feature Store REST API |
| `/grafana/` | Grafana | Prometheus 系统监控 |
| `/flink/` | Flink Web UI | 流作业状态 |
| `/kafka/` | Kafka UI | 消息队列管理 |

**Streamlit 三个页面**

- **💬 智能查询**：NL2SQL 多轮对话 + 自动图表（柱状图/折线图/散点图）+ RAG 知识问答 + NL2DDL 建视图
- **🤖 Agent 分析**：异常检测 / 自动处置看板 / Kappa 回放状态 / AI 主动洞察 / 自由分析
- **🗄️ 特征存储**：特征注册表浏览 / 在线查询 / PSI 漂移图表 / PIT 训练集构建

---

## 快速启动

### 前置要求

- Docker Desktop（内存建议 ≥ 8GB）
- DeepSeek API Key（[申请地址](https://platform.deepseek.com)）

### 1. 克隆 & 配置

```bash
git clone https://github.com/Nikka-ops/ai-data-warehouse.git
cd ai-data-warehouse

cp .env.example .env
# 编辑 .env，填入以下两项（其余保持默认即可）：
#   DEEPSEEK_API_KEY=sk-xxxx
#   CLICKHOUSE_PASSWORD=your_password
```

### 2. 一键启动

```bash
docker compose up -d
```

启动后自动完成：
1. Kafka + Zookeeper 就绪
2. ClickHouse 执行 11 个初始化 SQL（建库、建表、建视图）
3. Redis 启动（在线特征存储）
4. Kafka Producer 开始生产模拟巴西电商订单（10条/秒）
5. Flink 实时作业启动
6. Feature Store 服务启动（注册特征 → 计算 → 同步 Redis）
7. Superset 自动注册 35+ 数据集
8. 告警自动处置进程启动（30秒轮询）

### 3. 验证启动状态

```bash
# 查看所有容器状态
docker compose ps

# 查看特征存储初始化日志
docker compose logs feature-init

# 查看实时流处理日志
docker compose logs flink-stream
```

### 4. 本地开发模式（不使用 Docker）

```bash
pip install -r requirements.txt

# 终端 1：数据生产者
python kafka/producer.py --mode normal --rate 10

# 终端 2：Flink 流处理（Python 模拟模式，无需安装 PyFlink）
python flink/flink_stream_job.py --mode python

# 终端 3：特征计算管道
python feature_store/pipeline.py --loop 300

# 终端 4：Feature Store API
uvicorn api.feature_api:app --host 0.0.0.0 --port 8000

# 终端 5：AI 看板
streamlit run app/dashboard.py
```

---

## 服务地址

| 服务 | 地址 | 说明 |
|------|------|------|
| Superset BI | http://localhost | 数据看板（admin/admin）|
| AI 看板 | http://localhost/ai | Streamlit 智能查询 |
| Feature API | http://localhost/api/health | 特征服务健康检查 |
| Grafana | http://localhost/grafana | 系统监控（admin/admin）|
| Flink Web UI | http://localhost/flink | 流作业状态 |
| Kafka UI | http://localhost/kafka | 消息队列管理 |
| ClickHouse | http://localhost:8123/play | SQL 控制台 |

---

## 项目结构

```
ai-data-warehouse/
│
├── config.py                        # 集中配置（全部从 .env 读取，不硬编码密钥）
├── .env.example                     # 配置模板（.env 已 gitignore）
├── requirements.txt                 # Python 依赖
├── docker-compose.yml               # 16个容器一键编排
│
├── utils/
│   ├── logger.py                    # 结构化日志
│   └── retry.py                     # 指数退避重试（tenacity）
│
├── kafka/
│   └── producer.py                  # 巴西电商数据集模拟生产者
│
├── flink/
│   └── flink_stream_job.py          # 实时 + 回放双模式 Flink 作业
│                                    # 内嵌 AI 质量门控（规则 + LLM）
│
├── clickhouse/init/
│   ├── 01_init_tables.sql           # ODS 基础表
│   ├── 02_kafka_stream.sql          # Kafka Engine + 物化视图
│   ├── 03_flink_realtime.sql        # DWD/DWS/ADS 实时层
│   ├── 04_ai_etl.sql                # AI ETL 审计
│   ├── 05_ai_analytics.sql          # 主动洞察 + 预测存储
│   ├── 06_kappa_arch.sql            # Kappa 小时聚合 + 统一服务视图
│   ├── 07_alert_investigation.sql   # 告警排查记录
│   ├── 08_serving_layer.sql         # Kappa 服务视图（日趋势/品类）
│   ├── 09_remediation.sql           # 系统告警 + 自动处置审计
│   ├── 10_feature_store.sql         # Feature Store 完整 Schema
│   └── 11_ml_metadata.sql           # ML 元数据中心
│
├── features/                        # 特征 YAML 声明（唯一特征定义来源）
│   ├── user_features.yaml           # 用户行为特征（RFM 模型，6个）
│   ├── category_features.yaml       # 品类统计特征（4个）
│   ├── seller_features.yaml         # 卖家统计特征（4个）
│   ├── temporal_features.yaml       # 时间特征（3个）
│   └── generated/                   # LLM 自动生成特征（待人工审核）
│
├── feature_store/
│   ├── registry.py                  # YAML → ClickHouse 注册，全局单例
│   ├── offline_store.py             # 离线计算（服务端 INSERT INTO...SELECT）
│   ├── online_store.py              # Redis + ClickHouse 三级降级读取
│   ├── pit_join.py                  # ASOF JOIN PIT 训练集构建
│   ├── drift_monitor.py             # PSI 漂移检测（0.10/0.25 双阈值）
│   ├── pipeline.py                  # 5分钟调度：计算→写离线→同步在线
│   ├── dataset_builder.py           # PIT 训练集 → Parquet 导出 → 注册
│   └── auto_feature.py              # LLM 特征建议 + EXPLAIN 验证
│
├── api/
│   └── feature_api.py               # FastAPI，9个端点，/api/ 路由
│
├── ai_layer/
│   ├── tools.py                     # 13个 Agent 工具（唯一定义来源）
│   ├── nl2sql.py                    # NL2SQL（禁 DDL/DML，Schema 缓存）
│   ├── nl2ddl.py                    # NL2DDL（仅允许 ads.*/dws.* 视图）
│   ├── rag_engine.py                # RAG 知识库（ChromaDB）
│   ├── agents.py                    # ReAct Agent（异常/运营/Kappa/自由）
│   ├── alert_investigator.py        # 告警自动处置闭环
│   ├── insight_engine.py            # 主动洞察引擎
│   ├── forecaster.py                # 时序预测（Holt 双指数平滑）
│   └── session_manager.py           # 多轮对话会话管理
│
├── ai_etl/
│   └── ai_etl_agent.py              # AI 辅助 ETL 数据修复
│
├── app/
│   └── dashboard.py                 # Streamlit（3页：智能查询/Agent/特征存储）
│
├── superset/
│   ├── Dockerfile                   # Superset 镜像
│   ├── init_superset.py             # 自动注册 35+ 数据集
│   └── superset_config.py           # Superset 配置
│
├── monitoring/
│   ├── prometheus.yml               # Prometheus 抓取配置
│   └── grafana/                     # Grafana 仪表盘配置
│
├── nginx/
│   └── nginx.conf                   # 统一入口，6个上游服务路由
│
├── knowledge_base/
│   ├── 01_data_dict.md              # 数据字典（字段含义）
│   ├── 02_metrics.md                # 指标口径定义
│   └── 03_business_rules.md         # 业务规则说明
│
├── batch/
│   └── historical_loader.py         # 历史数据初始化加载
│
├── datasets/                        # PIT 训练集 Parquet 文件（运行时生成）
├── reports/                         # Agent 生成的分析报告（Markdown）
├── chroma_db/                       # ChromaDB 向量索引（运行时生成）
│
├── Dockerfile.dashboard             # Streamlit + AI 层镜像
└── Dockerfile.producer              # 数据生产者镜像
```

---

## Docker 服务清单

| 服务 | 镜像 | 说明 |
|------|------|------|
| `zookeeper` | confluentinc/cp-zookeeper | Kafka 协调服务 |
| `kafka` | confluentinc/cp-kafka | 消息队列，`RETENTION=-1` |
| `clickhouse` | clickhouse/clickhouse-server:24.3 | 核心存储，11个SQL自动初始化 |
| `redis` | redis:7.2-alpine | 在线特征存储，512mb allkeys-lru |
| `flink-jobmanager` | flink:1.18 | Flink 作业管理节点 |
| `flink-taskmanager` | flink:1.18 | Flink 计算节点 |
| `flink-stream` | Dockerfile.producer | 实时流处理作业 |
| `flink-replay` | Dockerfile.producer | 历史回放作业（按需启动）|
| `kafka-producer` | Dockerfile.producer | 巴西电商数据模拟生产者 |
| `feature-init` | Dockerfile.dashboard | 启动时注册 YAML 特征定义（restart:no）|
| `feature-pipeline` | Dockerfile.dashboard | 特征计算管道（`--loop 300`）|
| `feature-drift` | Dockerfile.dashboard | PSI 漂移检测（`--loop 3600`）|
| `feature-api` | Dockerfile.dashboard | FastAPI 特征服务 |
| `dashboard` | Dockerfile.dashboard | Streamlit AI 看板 |
| `ai-etl` | Dockerfile.dashboard | AI ETL 自动修复 |
| `alert-investigator` | Dockerfile.dashboard | 告警自动处置（30s轮询）|
| `superset` | superset/Dockerfile | Apache Superset BI |
| `grafana` | grafana/grafana | 监控看板 |
| `kafka-ui` | provectuslabs/kafka-ui | Kafka 管理界面 |
| `prometheus` | prom/prometheus | 指标采集 |
| `nginx` | nginx:1.25-alpine | 统一入口网关 |

---

## 技术亮点

### Kappa 统一流处理，消除批流二义性
同一套 Flink 代码通过 `--replay` 标志切换模式，历史重算和实时处理共享同一业务逻辑。`ReplacingMergeTree` 保证任意次回放结果幂等，消除 Lambda 架构的维护双倍代码的困境。

### Feature Store 三级降级，保障在线服务永不失败
在线查询优先读 Redis（微秒级），Redis miss 降级读 ClickHouse 离线层（毫秒级），ClickHouse 无数据则返回特征契约声明的默认值。三级降级保证推理服务在任何情况下都有值可用。

### ASOF JOIN PIT 正确性，从架构层面防数据泄漏
训练集构建使用 ClickHouse `ASOF JOIN`，在数据库引擎层面保证每个训练样本只能使用标签时间点之前的特征值，而非在应用层做时间过滤（容易出错）。

### 规则 + LLM 分层告警，节省 99% 推理成本
Flink 内嵌规则引擎（取消率阈值、价格阈值等）毫秒级检测，只有规则触发时才调用 DeepSeek 做根因分析。实际 LLM 调用量不到总检测量的 1%，同时保留了 AI 分析的深度。

### 安全设计
- 所有密钥通过 `.env` 注入，`.env` 已 gitignore，仓库只提交 `.env.example`
- NL2SQL 层硬拦截 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE
- NL2DDL 只允许 `CREATE VIEW`，且视图名强制以 `ads.` 或 `dws.` 开头，防止误操作生产表
