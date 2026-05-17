# 实时 AI 数仓系统

> 纯实时流处理架构：Kafka → Flink → ClickHouse，集成 NL2SQL、RAG 知识库、LangChain Agent 三大 AI 能力，支持自然语言查询实时数据。

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Flink](https://img.shields.io/badge/Apache_Flink-1.18-orange)](https://flink.apache.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow)](https://clickhouse.com)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-red)](https://kafka.apache.org)
[![LangChain](https://img.shields.io/badge/LangChain-0.3.25-green)](https://langchain.com)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

## 目录

- [系统架构](#系统架构)
- [技术栈](#技术栈)
- [数据流转](#数据流转)
- [AI 能力](#ai-能力)
- [快速启动](#快速启动)
- [项目结构](#项目结构)

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                  应用层（Streamlit）                           │
│   实时监控看板  │  NL2SQL 智能查询  │  Agent 自动分析          │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                    AI 能力层                                   │
│   NL2SQL（实时表路由）  │  RAG（知识库）  │  Agent（多步推理）  │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│              ClickHouse 实时数仓                               │
│                                                               │
│  stream.*          ODS              DWD            DWS / ADS  │
│  Kafka Engine  →  orders_stream  →  realtime_     realtime_   │
│  AI 告警           payments_stream   order_detail  minute_    │
│                                                   stats       │
│                                                   ads views   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                    实时处理层                                  │
│                                                               │
│  Kafka Producer          Flink（1分钟滚动窗口）                │
│  （模拟订单流）    →     ├── 订单+支付 JOIN → DWD 宽表         │
│  orders_stream           ├── 窗口聚合      → DWS 分钟统计     │
│  payments_stream         └── 异常检测      → AI 告警表        │
└─────────────────────────────────────────────────────────────┘
```

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 消息队列 | Apache Kafka + Zookeeper |
| 实时流处理 | Apache Flink 1.18（PyFlink Table API / Python 模拟模式）|
| 存储引擎 | ClickHouse 24.3（Kafka Engine + ReplacingMergeTree）|
| 大语言模型 | DeepSeek-Chat（OpenAI 兼容接口）|
| AI 框架 | LangChain 0.3.25（Tool Calling 模式）|
| 向量数据库 | ChromaDB + SentenceTransformers |
| 前端 | Streamlit（自动刷新实时看板）|
| 重试容错 | tenacity（指数退避）|
| 容器化 | Docker Compose |

---

## 数据流转

### 实时链路（全程自动，无需人工干预）

```
1. Kafka Producer 持续生产订单/支付消息
         ↓
2. ClickHouse Kafka Engine 自动消费 → ods.orders_stream / ods.payments_stream
         ↓
3. Flink 每1分钟一个滚动窗口：
   - 订单 + 支付 JOIN → dwd.realtime_order_detail
   - 聚合统计         → dws.realtime_minute_stats
   - 异常检测         → stream.ai_quality_alerts
         ↓
4. ClickHouse 实时视图（ads.*）供 NL2SQL 直接查询
         ↓
5. Streamlit 看板自动刷新展示
```

### ClickHouse 表说明

| 表 / 视图 | 数据来源 | 说明 |
|-----------|----------|------|
| `ods.orders_stream` | Kafka Engine 物化视图 | 原始订单流，24小时 TTL |
| `ods.payments_stream` | Kafka Engine 物化视图 | 原始支付流，24小时 TTL |
| `dwd.realtime_order_detail` | Flink JOIN | 订单+支付宽表 |
| `dws.realtime_minute_stats` | Flink 窗口聚合 | 分钟级统计，7天 TTL |
| `stream.ai_quality_alerts` | Flink 异常检测 | AI 告警，30天 TTL |
| `ads.realtime_hourly` | ClickHouse View | 今日小时趋势（自动 today() 过滤）|
| `ads.realtime_category_today` | ClickHouse View | 今日品类排行 |
| `ads.realtime_state_today` | ClickHouse View | 今日各州排行 |

---

## AI 能力

### NL2SQL — 自然语言查询实时数据

自动将中文问题转换为 ClickHouse SQL，路由到正确的实时表：

```
"最近10分钟订单量趋势"  →  SELECT ... FROM dws.realtime_minute_stats WHERE window_start >= now() - INTERVAL 10 MINUTE
"今日品类销售排行"      →  SELECT * FROM ads.realtime_category_today
"当前有哪些告警？"      →  SELECT ... FROM stream.ai_quality_alerts ORDER BY alert_time DESC
```

- Schema 缓存 TTL 5分钟（实时架构下缓存更短，及时感知结构变化）
- 内置 SQL 安全校验（禁止写操作）

### RAG — 知识库问答

向量检索业务知识库，回答概念定义类问题：

```
"order_status 有哪些状态？"  →  检索知识库 → 返回字段说明
"payment_type 怎么分类？"    →  检索知识库 → 返回支付类型说明
```

### Agent — 多步自动分析

三个专用 Agent，均基于 DeepSeek Function Calling：

| Agent | 功能 |
|-------|------|
| 异常检测 Agent | 检测分钟级流量异常（±2σ），查告警，生成检测报告 |
| 运营快报 Agent | 汇总今日各维度数据（小时/品类/州），生成运营快报 |
| 自由分析 Agent | 接受任意分析目标，自主决定查哪些表、如何分析 |

---

## 快速启动

### 前置要求

- Docker Desktop
- Python 3.11+
- DeepSeek API Key（[申请地址](https://platform.deepseek.com)）

### 1. 克隆 & 配置

```bash
git clone https://github.com/Nikka-ops/ai-data-warehouse.git
cd ai-data-warehouse

cp .env.example .env
# 编辑 .env，填入 DEEPSEEK_API_KEY 和 CLICKHOUSE_PASSWORD
```

### 2. 启动所有服务

```bash
docker-compose up -d
```

启动后服务会自动完成：
- Kafka 准备好接收消息
- ClickHouse 执行初始化 SQL（建库建表建物化视图）
- Kafka Producer 开始生产模拟订单（10条/秒）
- Flink 作业启动，开始处理实时流

### 3. 安装 Python 依赖（本地开发用）

```bash
pip install -r requirements.txt
```

### 4. 构建知识库（首次运行）

```bash
python ai_layer/rag_engine.py
```

### 5. 启动看板（本地开发）

```bash
streamlit run app/dashboard.py
```

> Docker 模式下看板已通过 `dashboard` 服务自动启动。

### 服务地址

| 服务 | 地址 | 说明 |
|------|------|------|
| AI 数仓看板 | http://localhost:8501 | 主界面 |
| Flink Web UI | http://localhost:8081 | 实时作业状态 |
| Kafka UI | http://localhost:8090 | 消息队列管理 |
| ClickHouse | http://localhost:8123/play | SQL 控制台 |

### 本地单独运行（不用 Docker）

```bash
# 终端 1：生产者
python kafka/producer.py --mode normal --rate 10

# 终端 2：Flink 流处理（Python 模拟模式）
python flink/flink_stream_job.py --mode python

# 终端 3：看板
streamlit run app/dashboard.py
```

---

## 项目结构

```
ai-data-warehouse/
│
├── config.py                        # 集中配置（从 .env 读取）
├── .env.example                     # 配置模板
│
├── utils/
│   ├── logger.py                    # 结构化日志
│   └── retry.py                     # 重试装饰器（tenacity）
│
├── kafka/
│   └── producer.py                  # 实时订单/支付模拟生产者
│
├── flink/
│   └── flink_stream_job.py          # Flink 流处理作业
│                                    # （PyFlink Table API，自动降级为 Python 模拟）
│
├── clickhouse/
│   └── init/
│       ├── 01_init_tables.sql       # 创建数据库
│       ├── 02_kafka_stream.sql      # Kafka Engine + ODS 落地表 + 物化视图
│       └── 03_flink_realtime.sql    # Flink 输出表 + 实时 ADS 视图
│
├── ai_layer/
│   ├── tools.py                     # Agent 工具（唯一定义来源）
│   ├── nl2sql.py                    # NL2SQL（实时表路由，Schema TTL缓存）
│   ├── rag_engine.py                # RAG 知识库
│   └── agents.py                    # 三个实时分析 Agent
│
├── app/
│   └── dashboard.py                 # Streamlit 实时看板（监控+查询+Agent）
│
├── knowledge_base/
│   ├── 01_data_dict.md              # 数据字典
│   ├── 02_metrics.md                # 指标口径
│   └── 03_business_rules.md         # 业务规则
│
├── reports/                         # Agent 生成的分析报告
│
├── docker-compose.yml               # 一键启动（Kafka + Flink + ClickHouse + 看板）
├── Dockerfile.producer              # 生产者 & Flink 作业镜像
├── Dockerfile.dashboard             # 看板镜像
└── requirements.txt
```

---

## 技术亮点

**Flink 替代 Python 轮询**
原有 `stream_processor.py` 用 `time.sleep()` 模拟分钟窗口，存在时钟漂移、无容错、单线程瓶颈问题。Flink 提供精确的 Processing Time 滚动窗口、Exactly-Once Checkpoint 语义和自动背压。无 PyFlink 环境时自动降级为 Python 模拟模式，功能等价。

**批流一体 NL2SQL → 纯实时 NL2SQL**
移除所有历史批量表描述，System Prompt 内置实时查询路由规则（"今天"→ `today()`，"最近N分钟"→ `INTERVAL`），`ads.*` 视图封装 `today()` 过滤，LLM 生成的 SQL 更简洁、出错率更低。

**规则 + AI 双层异常检测**
Flink 作业内嵌规则引擎（取消率 > 15%、价格 > R$3000 等）毫秒级响应，无需 LLM。仅规则触发时才调用 DeepSeek 生成原因分析，节省 99%+ 推理成本，同时保留 AI 分析能力。

**统一工具来源**
所有 Agent 工具定义集中于 `ai_layer/tools.py`，消除原来 `agents.py` 与 `agent_tools.py` 重复定义、行为不一致问题。`detect_realtime_anomaly` 工具取代通用的 `calculate_anomalies`，专为实时分钟级数据优化。
