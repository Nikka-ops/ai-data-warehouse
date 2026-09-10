# 🌊 实时数仓（Flink CDC + Doris）

> 电商实时数据仓库。用 **Flink CDC** 监听 MySQL 业务库的订单变更，
> 经 Kafka 分层、Flink SQL 加工后写入 **Apache Doris**，
> 实时输出 GMV、支付转化、地域与品类等经营指标，并支持用中文直接查数。
>
> 覆盖 watermark 乱序处理、主键幂等与 sequence 防覆盖、effectively-once 端到端一致性、
> BITMAP 精确去重、以及一套 Agent + MCP 的链路健康监控。

[![Flink](https://img.shields.io/badge/Apache_Flink-1.18-e6526f)](https://flink.apache.org)
[![Doris](https://img.shields.io/badge/Apache_Doris-2.1-1f6feb)](https://doris.apache.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-red)](https://kafka.apache.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-00758f)](https://mysql.com)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)

---

## 架构

```
   ┌──────────────────────────────────────────────────────────────┐
   │  MySQL 业务库 (mall)                                          │
   │  订单状态机：CREATED → PAID → SHIPPED → DELIVERED             │
   │  每次状态流转 = 一条 UPDATE = 一条 binlog                      │
   └───────────────────────────┬──────────────────────────────────┘
                               │  binlog (ROW / FULL)
   ┌───────────────────────────▼──────────────────────────────────┐
   │  Flink CDC  作业 rtdw-dwd                                     │
   │                                                               │
   │  增量快照算法（全量可并行、可 checkpoint、不锁表）              │
   │  维表 lookup join（改价下架实时感知）· 维度退化                 │
   │                                                               │
   │  事务事实（append-only）─┐        累积快照（changelog）─┐       │
   └──────────────────────────┼───────────────────────────────┼───┘
                              │                               │
        ┌─────────────────────▼──────────┐                    │
        │  Kafka  dwd_trade_order         │                    │
        │         dwd_trade_pay           │                    │
        │         dwd_trade_refund        │                    │
        └─────────────────────┬───────────┘                    │
                              │                                │
   ┌──────────────────────────▼────────────────────────┐       │
   │  Flink SQL  作业 rtdw-dws                          │       │
   │                                                    │       │
   │  TUMBLE 1min + CUMULATE 1day 窗口聚合               │       │
   │  GROUPING SETS 一次算三个粒度                        │       │
   │  watermark 5min 乱序容忍                            │       │
   │  CURRENT_WATERMARK() 分流迟到数据                    │       │
   │  RocksDB 状态后端 · 30s checkpoint · EXACTLY_ONCE   │       │
   └──────────────────────────┬─────────────────────────┘       │
                              │  Stream Load（主键幂等写入）      │
   ┌──────────────────────────▼─────────────────────────────────▼┐
   │                      Apache Doris                            │
   │                                                              │
   │  DWD  order_snapshot     Unique + MoW + sequence 防乱序覆盖   │
   │  DWS  trade_window_agg   Unique（Flink 已聚合 → 主键幂等）     │
   │       pay_window_agg     Unique                              │
   │       user_active_daily  Aggregate + BITMAP 跨天精确去重       │
   │  ADS  视图 + 异步物化视图 + 调度刷新的排行/对账结果表            │
   │  stream  迟到兜底 · 作业指标 · 质检告警 · 风控结果              │
   └──────┬────────────────────────────────────────────┬─────────┘
          │                                            │
   ┌──────▼──────────────────────┐      ┌──────────────▼─────────┐
   │  独立调度（Airflow）          │      │  实时看板 / NL2SQL      │
   │  ADS 刷新 · BITMAP · 对账     │      │  RAG · LangChain Agent │
   └─────────────────────────────┘      └────────────────────────┘
                          ┌───────────────────────────┐
                          │  Agent + MCP 链路监控      │
                          │  Flink REST · Kafka Lag   │
                          │  Doris 探活 → 健康卡 + 诊断 │
                          └───────────────────────────┘
```

---

## 技术栈

| 类别 | 技术 |
|------|------|
| 变更捕获 | Flink CDC 3.1（mysql-cdc，增量快照） |
| 流处理引擎 | Apache Flink 1.18（纯 Flink SQL） |
| 消息队列 | Apache Kafka 7.5（DWD 分层落 topic） |
| 存储 / OLAP | Apache Doris 2.1（MySQL 协议查询 + Stream Load 导入） |
| 业务库 | MySQL 8.0（binlog ROW / FULL） |
| 调度 | Apache Airflow 2.8（ADS 刷新与对账，与流作业运行时分离） |
| 大语言模型 | DeepSeek-Chat（OpenAI 兼容） |
| AI 框架 | LangChain 0.3（Tool Calling）+ MCP（运维工具） |
| 向量库 | ChromaDB + SentenceTransformers |
| 前端 | Streamlit + Plotly |

---

## 几个关键设计点

### 1. 为什么源头是 MySQL 而不是直接往 Kafka 灌 JSON

`mock/business_simulator.py` 只操作 MySQL 业务库，完全不知道 Kafka 和 Flink 的存在。
数据进入实时链路是 Flink CDC 读 binlog 的结果 —— 和真实生产环境里业务系统与数仓的
关系一致：**业务系统不为数仓而写**。

它的核心是一个订单状态机，不是「随机生成一条带终态的订单」。
每次状态流转都是业务库上的一条 UPDATE，于是 CDC 捕获到同一主键的多条变更。
这才让下游的两件事有真实意义：Unique Key 幂等、sequence 防乱序。

### 2. 事务事实 vs 累积快照：一个必须先想清楚的建模问题

CDC 读出来的是 changelog（带撤回语义），而窗口聚合 TVF 只接受 append-only 流。
直接把 CDC 流喂给窗口会报 `doesn't support consuming update changes`。

解法不是绕过报错，而是按数仓的方式把两类表分开：

| | 事务事实表 | 累积快照 |
|---|---|---|
| 来源 | insert-only 的 `order_detail` / `payment_info` | 会反复 UPDATE 的 `order_info` |
| 落到哪 | Kafka topic，供 DWS 窗口聚合 | Doris Unique 表，upsert |
| 回答 | 发生了多少次、多少钱（GMV） | 现在有多少处于某状态（转化率） |

### 3. 数据一致性：三件事叠起来才成立

- **Flink checkpoint（EXACTLY_ONCE 模式）** —— 算子状态与 Kafka 位点一起快照，故障恢复不丢不重算
- **Doris Unique Key + Merge-on-Write** —— 作业从 checkpoint 恢复会重放一段数据，按主键覆盖使重放幂等
- **Sequence Column** —— `update_time` 作为 sequence 列，从「后写入的赢」改成「事件时间大的赢」，
  迟到的 CREATED 盖不掉已经是 DELIVERED 的订单

三条合起来：同一份数据无论被处理几次、以什么顺序到达，最终都收敛到同一个正确值 ——
投递语义是 at-least-once，写入是幂等的，端到端效果是 effectively-once。

> **为什么不用 Doris sink 的两阶段提交（`sink.enable-2pc`）**
> 2PC 能把 sink 做到严格 Exactly-Once，但它要求导入 label 在 Doris 侧全局唯一且状态干净。
> 作业每次全新提交、失败重启、事务被中止后，连接器都要清理上一轮遗留事务，
> 本地反复起停时很容易在 checkpoint 1 就撞上 `Exist label abort finished`，之后每次重启
> 又从 checkpoint 1 开始，陷入死循环。既然所有目标表都是 Unique Key，幂等已经由模型保证，
> 2PC 带来的只是额外的故障面，所以这里刻意关掉。`flink/sql/00_init.sql` 里有完整说明。

**并且这个保证是被持续检验的** —— `pipelines/reconcile.py` 定时拿 MySQL 和 Doris
的同口径数字比一遍，差异落 `ads.reconcile_result`。

### 4. 迟到数据不静默丢失

watermark 给 5 分钟乱序容忍，这个范围内的乱序数据正常进窗口。
超出的进不了原窗口 —— 窗口 TVF 直接丢弃且不留痕迹。

DWS 作业用 `CURRENT_WATERMARK(create_time)` 把这批记录分流到 `stream.late_records`，
连同 Kafka 的 partition / offset 一起存。于是「丢数」变成「可量化、可按位点回放、可对账」。

> Flink 1.18 的窗口 TVF 没有 allowed-lateness 参数，乱序容忍度完全由 watermark 延迟决定 ——
> 设大了窗口出结果慢，设小了落兜底表的数据多。5 分钟是按上游乱序分布定的。

### 5. Doris 三种模型，按「谁做聚合」分工

| 模型 | 用在哪 | 为什么 |
|------|--------|--------|
| Unique + MoW | `dwd.order_snapshot`、两张窗口表 | Flink 已算完整窗口，按主键覆盖 → 重放幂等 |
| Aggregate + SUM/BITMAP | `dws.user_active_daily` | Doris 自己从明细滚上来，需要 SUM 语义 |

这个分界很容易踩坑：窗口表如果建成 Aggregate + SUM，作业重启重放时同一个窗口会被
再加一遍，GMV 直接翻倍 —— **Aggregate + SUM 不是幂等的**。
反过来，Aggregate 表的刷新任务必须「先删当天分区再写」，幂等性由刷新任务自己保证。

### 6. BITMAP：跨天精确去重

窗口表里的 `order_user_cnt` 是 Flink 在窗口状态里算的精确去重值，但**只在那个窗口内有效**。
一个用户在 10:01 和 10:05 各下一单，两个窗口各记一个，SUM 起来得 2。
跨窗口、跨天的去重，聚合结果本身根本合并不了。

常规做法是回明细 `COUNT(DISTINCT)`：查一次扫一次全量，问「最近 30 天独立用户」
就得扫 30 个分区的全部明细。

BITMAP 把「当天有哪些用户」这个集合本身存下来，之后
`BITMAP_UNION_COUNT(uv_bitmap)` 在压缩位图上做或运算，不回明细、结果精确。

### 7. 常驻流作业 vs 独立调度

| | 谁管生命周期 | 失败了怎么办 |
|---|---|---|
| `rtdw-dwd` / `rtdw-dws` | Flink 自己 | checkpoint 自动重启，从上次位点追赶积压 |
| ADS 刷新 / 对账 | Airflow | 重试，跑挂了不影响任何常驻进程 |

拆开的理由是故障隔离：调度器重启不会带断实时链路，批任务也不会占满 Flink 的 slot。

主链路本身也拆成两个作业而不是一个 STATEMENT SET —— DWS 的窗口聚合是有状态大户，
改窗口口径要重启；DWD 只是无状态清洗，没必要跟着停（停了等于业务库到数仓的入口断了）。

---

## Agent + MCP 链路监控（`ops_agent/`）

把「链路健不健康」这件事从「挨个打开 Flink UI / Kafka UI / Doris 控制台」
变成「问一句话」。

```
probes.py      三个探针
               Flink REST   作业状态 / checkpoint 失败 / 背压
               Kafka Lag    消费组积压（最新 offset - 已提交 offset）
               Doris        SHOW BACKENDS / 导入失败率 / 动态分区探活
     ↓
inspector.py   汇总成健康卡；有异常时才调 LLM 做归因，
               给出反压 / 数据倾斜 / 资源不足等方向的处置建议
     ↓
mcp_server.py  封装成 6 个标准 MCP 工具，任何 MCP 客户端都能调
```

**为什么用 MCP 而不是直接写成 LangChain @tool**：MCP 是模型无关、客户端无关的协议，
同一套工具 Claude Desktop 能用、Cursor 能用、自建 Agent 也能用，不锁定框架；
工具的 schema 与参数校验由协议标准化。

命令行直接用：

```bash
python -m ops_agent.inspector          # 打印健康卡
python -m ops_agent.inspector "现在链路有没有积压"   # 带问题就走 LLM 归因
python -m ops_agent.collect --interval 60   # 定时采集作业指标进 stream.job_metrics
```

注册成 MCP server（Claude Desktop / Cursor 的配置文件）：

```json
{
  "mcpServers": {
    "realtime-dw-ops": {
      "command": "python",
      "args": ["-m", "ops_agent.mcp_server"],
      "cwd": "E:/ai-data-warehouse"
    }
  }
}
```

---

## 快速启动

### 前置要求

- Docker Desktop，分配给 Linux VM **至少 7GB 内存**（Doris BE + Flink TaskManager 合计约 4GB）
- Python 3.11+
- PowerShell（Windows 自带的 5.1 即可，`pwsh` 7 也行；两个 Flink 脚本都做了兼容）
- DeepSeek API Key（可选，不配也能跑，AI 功能降级为规则模式）

> **Linux/WSL 必做**：Doris BE 要求 `vm.max_map_count >= 2000000`
> ```bash
> sudo sysctl -w vm.max_map_count=2000000
> ```
> Windows Docker Desktop 用户在 WSL 里执行同样命令。

### 1. 下载 Flink connector

```bash
pwsh flink/download_jars.ps1
```

拉四个 jar 到 `flink/lib/`：mysql-cdc、kafka、doris、mysql-jdbc。
版本号后缀必须和 Flink 主版本严格对上，脚本里已经钉死。

### 2. 启动基础设施

```bash
docker compose up -d
```

默认只起主链路必需的 7 个容器：Zookeeper、Kafka、MySQL、Doris FE/BE、Flink JM/TM。
Kafka UI 和 Airflow 放在可选 profile 里，主链路不依赖它们，需要时再起：

```bash
docker compose --profile tools up -d kafka-ui
```

```bash
docker compose --profile scheduler up -d airflow
```

Doris FE 首次启动约需 1~2 分钟。`doris-init` 容器会等 BE 注册完成后自动执行
`doris/init/*.sql` 建库建表。看进度：

```bash
docker logs -f ai_dw_doris_init
```

MySQL 会自动执行 `mysql/init/01_business_schema.sql` 建业务库与维表初始数据。

### 3. 安装依赖并配置

```bash
pip install -r requirements.txt
```

```bash
cp .env.example .env
```

然后按需填入 `DEEPSEEK_API_KEY`。其余默认值对应 docker-compose 里的服务，不用改。

### 4. 灌业务数据（写 MySQL，不碰 Kafka）

```bash
python mock/business_simulator.py --seed-dim
```

```bash
python mock/business_simulator.py --rate 20
```

保持这个进程一直开着。它每秒新建 20 单，并按状态机推进已有订单，
每次流转都是一条 UPDATE。加 `--burst` 可以周期性制造流量尖峰，用来观察背压。

### 5. 提交 Flink 作业

```bash
pwsh flink/submit.ps1
```

先提交 `rtdw-dwd`（CDC → 清洗打宽 → Kafka + 订单快照），
等 20 秒 topic 建出来后提交 `rtdw-dws`（窗口聚合 → Doris + 迟到分流）。

第一批窗口结果约 1 分钟后出现 —— Flink 要等 watermark 越过窗口终点才触发输出。

### 6. 看板与刷新

```bash
streamlit run app/realtime_dashboard.py
```

```bash
python pipelines/refresh_ads.py
```

排行榜和 BITMAP 日活由这个脚本刷新。生产上它挂在 Airflow 的 `ads_refresh` DAG 上，
每 10 分钟一次；本地手动跑一次即可看到维度下钻那一栏出数。

### 7. 可选组件

```bash
python quality/checker.py                 # AI 质检器（常驻，每分钟一轮）
python pipelines/reconcile.py             # 对账，结果落 ads.reconcile_result
python -m ops_agent.inspector             # 链路健康卡
streamlit run app/dashboard_v3.py         # NL2SQL + RAG + Agent 问答界面
python -m ai_layer.rag_engine             # 首次使用问答前先构建向量知识库
python eval/eval_nl2sql.py                # NL2SQL 评估集
```

### 访问地址

| 服务 | 地址 | 账号 |
|------|------|------|
| Flink Web UI | http://localhost:8081 | — |
| Kafka UI（可选，`--profile tools`） | http://localhost:8090 | — |
| Doris FE | http://localhost:8030 | root / 空 |
| Doris MySQL 协议 | `mysql -h127.0.0.1 -P9030 -uroot` | — |
| Airflow（可选，`--profile scheduler`） | http://localhost:8080 | admin / admin123 |
| 业务库 MySQL | `mysql -h127.0.0.1 -P3306 -uroot -proot123 mall` | — |

### 常用运维

```bash
pwsh flink/submit.ps1 -Status
```

```bash
pwsh flink/submit.ps1 -Stop
```

`-Stop` 会给每个作业各打一个 savepoint 再停 —— 直接 cancel 会丢掉算子状态，
下次启动只能从 Kafka 位点重放，窗口里攒了一半的数据全部作废。

---

## 项目结构

```
├── mysql/init/                 业务库建表 + 维表初始数据
├── mock/business_simulator.py  订单状态机模拟器（只写 MySQL）
├── flink/
│   ├── sql/00_init.sql         CDC 源 / Kafka 分层 / Doris sink 定义
│   ├── sql/10_dwd_job.sql      作业 rtdw-dwd：清洗打宽 + 订单快照
│   ├── sql/20_dws_job.sql      作业 rtdw-dws：窗口聚合 + 迟到分流
│   ├── ai_risk/                PyFlink AI 风控算子（可选，async I/O + 规则预筛）
│   ├── download_jars.ps1       拉 connector jar
│   └── submit.ps1              提交 / 查状态 / 带 savepoint 停止
├── doris/init/                 分层建表（DWD / DWS / ADS / stream）
├── pipelines/
│   ├── refresh_ads.py          ADS 刷新 + BITMAP 日活
│   ├── reconcile.py            业务库 ↔ 数仓对账
│   └── dag_*.py                Airflow DAG（与流作业运行时分离）
├── quality/checker.py          AI 质检器：规则先判、命中才调 LLM
├── ops_agent/                  链路监控探针 + 健康卡 + MCP server
├── app/
│   ├── realtime_dashboard.py   实时监控看板
│   └── dashboard_v3.py         NL2SQL + RAG + Agent 问答
├── ai_layer/
│   ├── nl2sql.py               自然语言转 Doris SQL
│   ├── sql_guard.py            只读校验 + 项目口径陷阱检查
│   ├── router.py               查数 / 问概念 的路由
│   ├── rag_engine.py           知识库向量检索
│   └── agents.py               异常分析 / 周报 / 自由分析 Agent
├── knowledge_base/             数据字典 · 指标口径 · 业务规则
├── eval/                       NL2SQL 评估集与评估脚本
└── common/doris_client.py      Doris 统一访问层（查询 + Stream Load）
```

---

## 数据

没有外部数据集依赖。业务数据全部由 `mock/business_simulator.py` 实时生成 ——
1000 个 SKU、5000 个用户、15 个三级品类、26 个省份 7 个大区，
订单按状态机推进，时间被压缩（真实世界的「下单到收货三天」在这里是几十秒），
否则跑一整天也看不到一个完整生命周期。

---

## License

MIT
