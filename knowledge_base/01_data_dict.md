# 数据字典

## 数据库总览

本数仓是**纯实时链路**：数据由 Flink CDC 从 MySQL 业务库（库名 `mall`）的 binlog 实时捕获，
经 Kafka 分层、Flink SQL 加工后写入 Apache Doris。没有历史批数据 ——
能查到的时间范围就是链路运行以来的这几天。

四个数据库层次：

| 库 | 定位 | 谁写入 |
|----|------|--------|
| `dwd` | 明细层：订单累积快照 | Flink CDC 作业（秒级） |
| `dws` | 汇总层：窗口聚合 + BITMAP 日活 | Flink 窗口作业 / ADS 刷新任务 |
| `ads` | 应用层：视图、物化视图、排行结果表 | 视图查询即算 / 调度刷新 |
| `stream` | 运维层：迟到数据、作业指标、告警 | Flink 分流 / 采集脚本 |

**ODS 层不在 Doris**。实时链路的 ODS 落在 Kafka 的 `dwd_trade_order` /
`dwd_trade_pay` / `dwd_trade_refund` 三个 topic 上，是给下游流作业消费的中间态，
不是给人查的。每多落一次库就多一次写入与读取，延迟逐层累加。

**维表也不在 Doris**。Flink 直接用 mysql-cdc 读业务库的 `sku_info` /
`base_category3` / `base_province` 做 lookup join，改价、下架能被实时感知。

---

## DWD 层

### dwd.order_snapshot — 订单累积快照

一个订单一行，记录它**当前**的状态。同一个 `order_id` 会随着状态流转被 CDC
写入很多次，靠 Unique Key 按主键覆盖 + `update_time` 做 sequence 列收敛。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| create_date | DATE | 下单日期，分区列 |
| order_id | BIGINT | 订单ID，主键 |
| update_time | DATETIME(3) | 业务库更新时间，**sequence 列**，事件时间大的版本获胜 |
| user_id | BIGINT | 用户ID |
| province_name | VARCHAR | 省份名（已在 Flink 侧维度退化） |
| region_name | VARCHAR | 大区名：华北/华东/华南/华中/西南/西北/东北 |
| order_status | VARCHAR | 订单状态，取值见业务规则手册 |
| total_amount | DECIMAL(16,2) | 订单总额 |
| activity_reduce / coupon_reduce | DECIMAL(16,2) | 活动优惠 / 优惠券优惠 |
| freight_amount | DECIMAL(16,2) | 运费 |
| create_time / payment_time / ship_time / receive_time | DATETIME(3) | 各状态发生时刻 |
| pay_lag_seconds | INT | 下单到支付耗时（秒），未支付为 NULL |
| ingest_time | DATETIME(3) | Flink 落库时间 |

**这张表能回答、而窗口聚合表回答不了的问题**：现在有多少订单处于某状态、
取消率、支付转化率、下单到支付要多久。因为它记录的是状态，不是事件计数。

---

## DWS 层

### dws.trade_window_agg — 交易域窗口聚合

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| stat_date | DATE | 统计日期，分区列 |
| window_type | VARCHAR | `TUMBLE_1M` 分钟滚动窗口 / `CUMULATE_1D` 当日累计窗口 |
| window_start / window_end | DATETIME(3) | 窗口起止 |
| category1_name | VARCHAR | 一级品类，**`ALL` 表示不分品类的汇总行** |
| region_name | VARCHAR | 大区，**`ALL` 表示不分大区的汇总行** |
| order_cnt | BIGINT | 订单数（窗口内去重） |
| order_user_cnt | BIGINT | 下单用户数，**仅在单个窗口内有效** |
| sku_num | BIGINT | 商品件数 |
| order_amount | DECIMAL(18,2) | **GMV，字段名不叫 gmv** |
| max_order_price | DECIMAL(16,2) | 窗口内最高单价 |
| abnormal_price_cnt | BIGINT | 成交价显著偏离标价的条数 |
| max_lag_seconds / avg_lag_seconds | INT | 窗口内端到端延迟 |

### dws.pay_window_agg — 支付域窗口聚合

维度是 `payment_type`（`ALL` 表示汇总）。字段：`pay_cnt`、`pay_user_cnt`、
`pay_amount`、`max_lag_seconds`、`avg_lag_seconds`。

### dws.user_active_daily — 用户日活（BITMAP）

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| dt | DATE | 日期 |
| region_name | VARCHAR | 大区，`ALL` 表示全国 |
| order_cnt / order_amount | SUM 聚合 | 当日订单数 / 成交额 |
| uv_bitmap | BITMAP | 当日下单用户位图 |
| pay_uv_bitmap | BITMAP | 当日支付用户位图 |

BITMAP 列不能直接 SELECT，取基数用 `BITMAP_UNION_COUNT(uv_bitmap)`。
由 `pipelines/refresh_ads.py` 定时从 `dwd.order_snapshot` 刷新。

---

## ADS 层

| 对象 | 类型 | 说明 |
|------|------|------|
| ads.v_today_trade_kpi | 视图 | 今日累计交易 KPI，一次点查 |
| ads.v_today_pay_kpi | 视图 | 今日累计支付 KPI |
| ads.v_today_conversion | 视图 | 支付转化漏斗，来自订单快照 |
| ads.v_minute_trend | 视图 | 近 2 小时分钟趋势 |
| ads.mv_hourly_trend | 异步物化视图 | 小时趋势，每 5 分钟刷新 |
| ads.category_rank | 结果表 | 当日品类排行，调度刷新 |
| ads.region_rank | 结果表 | 当日大区排行，`uv` 列来自 BITMAP |
| ads.reconcile_result | 结果表 | 业务库与数仓对账差异 |

---

## stream 运维层

| 表 | 说明 |
|----|------|
| stream.late_records | 超出 watermark 容忍度的记录，带 Kafka partition/offset，可按位点回放 |
| stream.ai_quality_alerts | 规则命中的质检告警及 LLM 处置建议 |
| stream.job_metrics | Flink 作业状态、背压、checkpoint 耗时、重启次数 |
| stream.ai_risk_result | 实时 AI 风控研判结果，只存需复核的订单 |
