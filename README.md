# 🤖 AI 智能数仓系统

> 基于巴西电商真实数据，从零构建 AI 增强数据仓库。覆盖离线批处理与实时流处理双链路，集成 NL2SQL、RAG 知识库、LangChain Agent 三大 AI 能力。

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow)](https://clickhouse.com)
[![LangChain](https://img.shields.io/badge/LangChain-0.3.25-green)](https://langchain.com)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-red)](https://kafka.apache.org)
[![License](https://img.shields.io/badge/License-MIT-lightgrey)](LICENSE)

---

## 📋 目录

- [项目简介](#项目简介)
- [系统架构](#系统架构)
- [技术栈](#技术栈)
- [六个阶段](#六个阶段)
- [核心功能演示](#核心功能演示)
- [快速启动](#快速启动)
- [项目结构](#项目结构)
- [数据集](#数据集)

---

## 项目简介

本项目以 **Kaggle 巴西电商平台 Olist 真实数据**（112,650 条订单）为底座，分六个阶段逐步构建一个具备 AI 能力的完整数据仓库系统。

**核心价值：让不懂 SQL 的业务人员也能直接用中文查数据、问问题、获取分析洞察。**

| 指标 | 数值 |
|------|------|
| 总 GMV | R$ 13,591,644 |
| 订单总数 | 98,666 单 |
| 独立用户数 | 97,729 人 |
| 平均客单价 | R$ 132.71 |
| 知识库文本块 | 36 个 |
| Agent 最大推理步数 | 10 步 |

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层（Streamlit）                      │
│   智能问答  │  异常检测Agent  │  自动周报Agent  │  自由分析Agent│
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      AI 能力层                               │
│   NL2SQL（DeepSeek）  │  RAG（ChromaDB）  │  Agent（LangChain）│
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                   数仓存储层（ClickHouse）                    │
│  ADS（应用层）│ DWS（汇总层）│ DWD（明细层）│ ODS（原始层）    │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│              数据接入层                                       │
│    批量 ETL（Python）         Kafka 实时流                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 存储引擎 | ClickHouse 24.3（列式 OLAP） |
| 数据建模 | dbt-clickhouse |
| 工作流调度 | Apache Airflow 2.8 |
| 消息队列 | Apache Kafka + Zookeeper |
| 大语言模型 | DeepSeek-Chat（OpenAI 兼容） |
| AI 框架 | LangChain 0.3.25（Tool Calling 模式） |
| 向量数据库 | ChromaDB + SentenceTransformers |
| Embedding 模型 | paraphrase-multilingual-MiniLM-L12-v2 |
| 前端界面 | Streamlit 1.33 |
| 容器化 | Docker Compose |

---

## 六个阶段

### 阶段一：数仓基础建设 ✅
基于 ClickHouse 构建 ODS/DWD/DWS/ADS 四层数仓，使用 ReplacingMergeTree 引擎保障 ETL 幂等性，Airflow 实现全链路自动化调度。

```
Kaggle CSV → etl_ods.py → etl_dwd.py → etl_dws_ads.py → verify.py
344,483行     112,650行     19,606行      583行
```

### 阶段二：NL2SQL 自然语言查询 ✅
基于 DeepSeek 实现中文 → ClickHouse SQL 转换。动态注入真实表结构，通过正负面约束解决字段歧义，5 个业务查询场景全部通过。

```python
# 示例：用中文查数据
result = nl2sql("每个月的GMV是多少？")
# → 自动生成 SQL → 执行 → 返回结果 + AI 洞察
```

### 阶段三：RAG 知识库问答 ✅
构建包含数据字典、指标口径、业务规则的向量知识库（36 个文本块），实现智能路由：自动判断"查数据"还是"问概念"。6 个知识问答准确率 100%。

### 阶段四：LangChain Agent ✅
基于 **Tool Calling 模式**（解决 ReAct + DeepSeek 兼容性问题）构建三个专用 Agent：

| Agent | 步骤数 | 核心能力 |
|-------|--------|----------|
| 销售异常检测 | 5步 | 自动识别黑五峰值（偏差10.48倍），发现预热→爆发→余热三阶段 |
| 自动周报生成 | 9步 | 多维数据查询，主动发现数据异常并标注预警 |
| 自由分析 | 10步 | 自主追加3个分析维度，识别狂欢节低谷和春季旺季 |

### 阶段五：AI 数仓建设助手 ✅
上传任意 CSV → AI 自动识别字段含义 → 生成 ODS 建表 SQL → 建表写入 → AI 生成 ETL 逻辑 → 数据质量检测 → 生成 dbt 模型文件。

### 阶段六：Kafka 实时流处理 ✅
Python 生产者模拟实时订单流 → Kafka → ClickHouse Kafka 引擎自动消费（秒级）→ 物化视图落地 → 分钟级聚合 → AI 异常检测 → 告警写入。

```
累计流入：4,247+ 条  |  分钟聚合：~171单/分钟  |  GMV：R$55,421/分钟
```

---

## 核心功能演示

### NL2SQL 查询
```
用户：每个月的GMV是多少？
系统：[自动生成SQL] SELECT ym, round(gmv,0) FROM ads.monthly_kpi ORDER BY ym
      [执行结果] 24个月数据，2017-11月峰值...
      [AI洞察] 黑色星期五当天GMV是日均的6.8倍...
```

### RAG 知识问答
```
用户：GMV和销售额有什么区别？
系统：[检索知识库] 相似度0.693...
      GMV包含所有状态订单（含取消），反映平台交易规模；
      实际销售额只计算delivered订单，反映真实成交。
```

### Agent 自动分析
```
用户：分析2018年上半年销售趋势
Agent步骤1：查知识库，了解GMV定义
Agent步骤2：查月度数据（发现SQL格式错误，自动纠错）
Agent步骤3：追加查订单状态分布（自主决策）
Agent步骤4：追加查配送时效（自主决策）
Agent步骤5：追加查取消率（自主决策）
...
Agent结论：2月受巴西狂欢节影响GMV最低，4-5月春季旺季...
```

---

## 快速启动

### 前置要求
- Docker Desktop
- Python 3.11+
- DeepSeek API Key（[申请地址](https://platform.deepseek.com)）

### 1. 启动基础服务
```bash
git clone https://github.com/你的用户名/ai-data-warehouse.git
cd ai-data-warehouse
docker-compose up -d
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 配置环境变量
```bash
# Windows
set DEEPSEEK_API_KEY=your_api_key_here

# Mac/Linux
export DEEPSEEK_API_KEY=your_api_key_here
```

### 4. 初始化数仓
```bash
# 下载数据集（需要 Kaggle 账号）
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# 将 CSV 文件放入 data/raw/

# 运行 ETL
python pipelines/etl_ods.py
python pipelines/etl_dwd.py
python pipelines/etl_dws_ads.py
python pipelines/verify.py  # 验收
```

### 5. 构建知识库
```bash
python ai_layer/rag_engine.py
```

### 6. 启动 AI 数仓助手
```bash
# 历史数据查询 + RAG + Agent
streamlit run app/dashboard_v3.py

# 实时流处理
python kafka/producer.py --rate 5
python kafka/stream_processor.py
```

### 访问地址

| 服务 | 地址 |
|------|------|
| AI 数仓助手 | http://localhost:8501 |
| Kafka UI | http://localhost:8090 |
| Airflow | http://localhost:8080 |
| ClickHouse Play | http://localhost:8123/play |

---

## 项目结构

```
ai-data-warehouse/
├── clickhouse/
│   └── init/
│       ├── 01_init_tables.sql      # 历史数仓建表
│       └── 02_kafka_stream.sql     # 流式表结构
├── pipelines/
│   ├── etl_ods.py                  # ODS 层加载
│   ├── etl_dwd.py                  # DWD 层加工
│   ├── etl_dws_ads.py              # DWS/ADS 层聚合
│   ├── verify.py                   # 四层验收
│   └── dag_daily_pipeline.py       # Airflow DAG
├── knowledge_base/
│   ├── 01_data_dict.md             # 数据字典
│   ├── 02_metrics.md               # 指标口径
│   └── 03_business_rules.md        # 业务规则
├── ai_layer/
│   ├── nl2sql.py                   # 自然语言转SQL
│   ├── rag_engine.py               # RAG 知识库
│   ├── agent_tools.py              # Agent 工具集
│   └── agents.py                   # 三个专用 Agent
├── kafka/
│   ├── producer.py                 # 实时订单生产者
│   ├── stream_processor.py         # AI 流处理器
│   └── dag_realtime_stream.py      # 实时调度 DAG
├── app/
│   ├── dashboard.py                # v1：NL2SQL
│   ├── dashboard_v2.py             # v2：+RAG
│   ├── dashboard_v3.py             # v3：+Agent
│   └── realtime_dashboard.py       # 实时监控看板
├── reports/                        # Agent 生成的分析报告
├── docker-compose.yml              # 一键启动
└── requirements.txt                # Python 依赖
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

1. **Tool Calling 解决 LLM 兼容性问题**：ReAct 框架与 DeepSeek 输出格式不匹配导致死循环，改用 Function Calling JSON 协议彻底解决
2. **Agent 自主纠错能力**：SQL 中文别名报错后 Agent 自动换英文别名重试，无需人工干预
3. **规则+AI 双重检测**：流处理中正常情况0次LLM调用，规则触发异常后才调 AI，节省99%+推理成本
4. **批流一体架构**：历史数据和实时数据共用同一套表结构，NL2SQL 透明访问两套数据

---

## License

MIT License - 详见 [LICENSE](LICENSE) 文件
