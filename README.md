# AI 智能数仓系统

> 基于巴西电商真实数据构建的 AI 增强数据仓库，覆盖离线批处理与 Flink 实时流双链路，集成 NL2SQL、RAG 知识库、LangChain Agent 三大 AI 能力，支持历史与实时数据统一查询。

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow)](https://clickhouse.com)
[![Flink](https://img.shields.io/badge/Apache_Flink-1.18-orange)](https://flink.apache.org)
[![LangChain](https://img.shields.io/badge/LangChain-0.3.25-green)](https://langchain.com)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-red)](https://kafka.apache.org)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

## 目录

- [项目简介](#项目简介)
- [系统架构](#系统架构)
- [技术栈](#技术栈)
- [核心功能](#核心功能)
- [快速启动](#快速启动)
- [项目结构](#项目结构)
- [数据集](#数据集)
- [技术亮点](#技术亮点)

---

## 项目简介

本项目以 **Kaggle 巴西电商平台 Olist 真实数据**（112,650 条订单）为底座，构建一个具备 AI 能力的完整数据仓库系统。

**核心价值：让不懂 SQL 的业务人员用中文查历史数据、看实时动态、获取 AI 分析洞察。**

| 指标 | 数值 |
|------|------|
| 历史订单总数 | 98,666 单 |
| 历史总 GMV | R$ 13,591,644 |
| 独立用户数 | 97,729 人 |
| 平均客单价 | R$ 132.71 |
| 实时处理延迟 | < 1 分钟（Flink 窗口） |
| NL2SQL 可查表 | 9 张（5 张历史 + 4 张实时） |

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     应用层（Streamlit）                           │
│   智能问答  │  异常检测 Agent  │  自动周报 Agent  │  自由分析 Agent│
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                        AI 能力层                                  │
│  NL2SQL（历史+实时路由）│  RAG（ChromaDB）│  Agent（LangChain）   │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                  数仓存储层（ClickHouse）                          │
│                                                                   │
│  ┌─ 历史批量链路 ──────────────────┐  ┌─ 实时流链路 ────────────┐ │
│  │ ADS │ DWS │ DWD │ ODS（历史）  │  │ ODS流 │ DWD实时 │ DWS分钟│ │
│  └──────────────────────────────┘  └────────────────────────┘ │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                       数据接入层                                   │
│                                                                   │
│   批量 ETL（Auto ETL / Airflow）     Kafka 实时流                  │
│   CSV → ODS → DWD → DWS/ADS         Producer → Kafka             │
│   每日凌晨自动调度（幂等）                      ↓                  │
│                                      Flink（1分钟窗口聚合）        │
│                                      ↓            ↓              │
│                                  DWS分钟统计   DWD实时宽表         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 存储引擎 | ClickHouse 24.3（列式 OLAP） |
| 实时流处理 | Apache Flink 1.18（PyFlink Table API）|
| 消息队列 | Apache Kafka + Zookeeper |
| 工作流调度 | APScheduler（内置）/ Apache Airflow 2.8 |
| 大语言模型 | DeepSeek-Chat（OpenAI 兼容接口） |
| AI 框架 | LangChain 0.3.25（Tool Calling 模式） |
| 向量数据库 | ChromaDB + SentenceTransformers |
| Embedding 模型 | paraphrase-multilingual-MiniLM-L12-v2 |
| 前端界面 | Streamlit 1.33 |
| 重试容错 | tenacity（指数退避） |
| 容器化 | Docker Compose |

---

## 核心功能

### 1. NL2SQL — 历史与实时统一查询

NL2SQL 模块同时感知**历史批量表**和**实时流表**，根据问题语义自动路由：

| 问题类型 | 路由目标 | 示例 |
|----------|----------|------|
| "历史/趋势/月度" | dws/ads 批量层 | 每月GMV趋势 |
| "今天/实时/当前" | ods.orders_stream | 今日实时销售额 |
| "分钟级流量" | dws.realtime_minute_stats | 最近30分钟订单量 |
| "异常告警" | stream.ai_quality_alerts | 有哪些实时告警 |

```python
# 查历史
result = nl2sql("2018年各月GMV环比增长率是多少？")
# → 自动查 ads.monthly_kpi

# 查实时
result = nl2sql("今天各品类的实时销售额排行")
# → 自动查 ods.orders_stream WHERE event_time >= today()
```

Schema 缓存带 TTL（默认1小时），表结构变更后自动刷新。

### 2. RAG 知识库问答

向量检索业务知识库（数据字典、指标口径、业务规则），回答"概念型"问题：

```
用户：GMV 和销售额有什么区别？
系统：[检索相似度 0.693] GMV 包含所有状态订单（含取消），
      反映平台交易规模；实际销售额只计算 delivered 订单，反映真实成交。
```

内置相似度阈值过滤（distance > 0.7 时直接返回"无此信息"，避免 AI 幻觉）。

### 3. LangChain Agent — 三种专用分析模式

基于 Tool Calling 模式（解决 ReAct 与 DeepSeek 兼容性问题）：

| Agent | 能力 | 支持模式 |
|-------|------|----------|
| 异常分析 Agent | ±2σ 统计检测 + AI 原因分析 | 历史 / 实时双模式 |
| 自动周报 Agent | 多维查询 + 今日实时快照 | 历史趋势 + 实时 |
| 自由分析 Agent | 自主追加分析维度，历史+实时均可查 | 全表访问 |

工具定义集中于 `ai_layer/tools.py`（唯一来源），Agent 直接导入，无重复实现。

### 4. Flink 实时流处理

替代原有 Python 轮询方案，实现真正的有状态流计算：

```
Kafka(orders_stream + payments_stream)
          │
          ▼
    Flink（1分钟滚动窗口）
    ├── 聚合：order_cnt / total_gmv / avg_price / unique_customers
    ├── JOIN：订单 + 支付关联宽表
    └── 检测：规则引擎（取消率/高价异常）+ AI 分析建议
          │
          ▼
    ClickHouse
    ├── dws.realtime_minute_stats（分钟聚合）
    ├── dwd.realtime_order_detail（DWD 宽表）
    └── stream.ai_quality_alerts（告警）
```

**自动降级**：PyFlink 运行时不可用时，无缝切换为 Python 模拟模式（功能等价，适合本地开发）。

### 5. Auto ETL — 自动化调度

无需 Airflow 即可运行完整 ETL 流水线：

```bash
# 持续调度（默认：01:00 ODS / 02:00 DWD / 03:00 ADS，巴西时区）
python pipelines/auto_etl.py

# 立即执行一次全量 ETL
python pipelines/auto_etl.py --once

# 手动触发单层
python pipelines/auto_etl.py --stage dwd
```

ETL 全链路幂等：使用 `ReplacingMergeTree + OPTIMIZE FINAL` 替代 `TRUNCATE + INSERT`，重跑安全，不丢数据。

---

## 快速启动

### 前置要求

- Docker Desktop
- Python 3.11+
- DeepSeek API Key（[申请地址](https://platform.deepseek.com)）

### 1. 克隆项目 & 配置环境变量

```bash
git clone https://github.com/Nikka-ops/ai-data-warehouse.git
cd ai-data-warehouse

# 复制配置模板
cp .env.example .env

# 编辑 .env，填入你的 API Key 和密码
# 必填项：
#   DEEPSEEK_API_KEY=sk-your-key
#   CLICKHOUSE_PASSWORD=your-password
```

### 2. 启动基础服务

```bash
# 启动 ClickHouse / Kafka / Flink / Airflow
docker-compose up -d

# 查看服务状态
docker-compose ps
```

### 3. 安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 4. 准备数据集

从 [Kaggle Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) 下载 CSV 文件，放入 `data/raw/` 目录：

```
data/raw/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_customers_dataset.csv
└── olist_products_dataset.csv
```

### 5. 初始化数仓（二选一）

**方式 A：一次性全量执行**
```bash
python pipelines/auto_etl.py --once
```

**方式 B：分步执行（便于排查）**
```bash
python pipelines/etl_ods.py      # ODS 原始层
python pipelines/etl_dwd.py      # DWD 明细层
python pipelines/etl_dws_ads.py  # DWS/ADS 聚合层
python pipelines/verify.py       # 四层验收
```

### 6. 构建知识库

```bash
python ai_layer/rag_engine.py
```

### 7. 启动实时流处理

```bash
# 终端 1：启动 Kafka 生产者（模拟实时订单）
python kafka/producer.py --mode normal --rate 10

# 终端 2：启动 Flink 流处理作业
# （PyFlink 已安装则用 Flink 模式，否则自动降级为 Python 模拟模式）
python flink/flink_stream_job.py
```

### 8. 启动看板

```bash
streamlit run app/dashboard.py
```

### 服务访问地址

| 服务 | 地址 | 说明 |
|------|------|------|
| AI 数仓看板 | http://localhost:8501 | NL2SQL + RAG + Agent |
| Flink Web UI | http://localhost:8081 | 实时作业监控 |
| Kafka UI | http://localhost:8090 | 消息队列管理 |
| Airflow | http://localhost:8080 | DAG 调度管理 |
| ClickHouse | http://localhost:8123/play | SQL 控制台 |

---

## 项目结构

```
ai-data-warehouse/
│
├── config.py                        # 集中配置（全部从 .env 读取）
├── .env.example                     # 配置模板（复制为 .env 使用）
│
├── utils/
│   ├── logger.py                    # 结构化日志（替代 print）
│   └── retry.py                     # 重试装饰器（tenacity）
│
├── ai_layer/
│   ├── tools.py                     # Agent 工具唯一定义来源
│   ├── nl2sql.py                    # NL2SQL（历史+实时路由，Schema TTL缓存）
│   ├── rag_engine.py                # RAG 知识库（相似度阈值过滤）
│   └── agents.py                    # 三个专用 Agent（从 tools.py 导入）
│
├── flink/
│   └── flink_stream_job.py          # Flink 实时流作业（自动降级为 Python 模拟）
│
├── kafka/
│   ├── producer.py                  # 实时订单模拟生产者
│   └── stream_processor.py          # 历史 Python 轮询处理器（已由 Flink 替代）
│
├── pipelines/
│   ├── auto_etl.py                  # 自动 ETL 调度（APScheduler）
│   ├── etl_ods.py                   # ODS 层加载（幂等）
│   ├── etl_dwd.py                   # DWD 层加工（幂等）
│   ├── etl_dws_ads.py               # DWS/ADS 层聚合（幂等）
│   ├── verify.py                    # 四层验收
│   └── dag_daily_pipeline.py        # Airflow DAG（可选）
│
├── clickhouse/
│   └── init/
│       ├── 01_init_tables.sql       # 历史数仓建表（ODS/DWD/DWS/ADS）
│       ├── 02_kafka_stream.sql      # Kafka 引擎表 + 物化视图
│       └── 03_flink_realtime.sql    # Flink 输出表 + 实时 ADS 视图
│
├── knowledge_base/
│   ├── 01_data_dict.md              # 数据字典
│   ├── 02_metrics.md                # 指标口径
│   └── 03_business_rules.md         # 业务规则
│
├── app/
│   ├── dashboard.py                 # 主看板（NL2SQL + RAG + Agent）
│   └── realtime_dashboard.py        # 实时监控看板
│
├── reports/                         # Agent 生成的分析报告（Markdown）
│
├── docker-compose.yml               # 一键启动（含 Flink 集群）
├── Dockerfile.producer              # 生产者镜像
├── Dockerfile.dashboard             # 看板镜像
└── requirements.txt                 # Python 依赖
```

---

## 数据集

使用 [Kaggle Olist 巴西电商数据集](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)，包含 2016-2018 年真实交易数据：

| 文件 | 描述 | 行数 |
|------|------|------|
| olist_orders_dataset.csv | 订单主表 | 99,441 |
| olist_order_items_dataset.csv | 订单商品明细 | 112,650 |
| olist_customers_dataset.csv | 用户信息 | 99,441 |
| olist_products_dataset.csv | 商品信息 | 32,951 |

---

## 技术亮点

**1. 批流一体 NL2SQL**
同一套 NL2SQL 引擎，根据问题语义自动路由历史表或实时表，业务人员无需感知底层架构差异。Schema 缓存带 TTL 失效机制，表结构变更后自动刷新，无需重启。

**2. Flink 替代 Python 轮询**
原方案用 `time.sleep()` 模拟时间窗口，时钟漂移、无 Checkpoint、单线程处理。Flink 提供精确的 Processing Time 窗口语义、Exactly-Once 消息保证和自动背压。开发环境无 PyFlink 时自动降级为 Python 模拟模式，无需改代码。

**3. ETL 幂等改造**
将 `TRUNCATE + INSERT`（非原子，中途失败导致数据丢失）改为 `INSERT + OPTIMIZE FINAL`，利用 `ReplacingMergeTree` 版本机制实现安全的幂等重跑。

**4. 工具唯一来源**
Agent 工具统一定义于 `ai_layer/tools.py`，消除原来 `agents.py` 与 `agent_tools.py` 重复实现、行为不一致的问题（原有返回行数 30 vs 50 的差异）。

**5. Tool Calling 解决 LLM 兼容性**
ReAct 框架与 DeepSeek 输出格式不匹配会导致解析死循环，改用 Function Calling JSON 协议彻底解决，Agent 中间步骤清晰可审计。

**6. 规则 + AI 双层异常检测**
Flink 流处理中，规则引擎（取消率/价格异常）毫秒级响应，无需调用 LLM。仅在规则触发告警时才调用 DeepSeek 生成原因分析，节省 99%+ 推理成本。

---

## License

MIT License - 详见 [LICENSE](LICENSE) 文件
