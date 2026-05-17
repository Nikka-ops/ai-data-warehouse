# 数据字典

## 系统架构概览

本系统为**纯实时流处理架构**，数据链路如下：

```
Kafka 生产者（模拟实时订单）
    ↓
Kafka Topics: orders_stream / payments_stream
    ↓
ClickHouse Kafka Engine（自动消费）→ ODS 落地表
    ↓
Flink（1分钟滚动窗口处理）→ DWD 宽表 / DWS 分钟统计 / AI 告警
    ↓
ClickHouse 实时视图（ads.*）→ NL2SQL / Dashboard 查询
```

数据无历史批量导入，所有数据来自实时流，数据新鲜度 < 1 分钟。

---

## ODS 层（实时原始落地层）

### ods.orders_stream — 实时订单流

由 ClickHouse Kafka Engine 自动消费 `orders_stream` topic 写入。每条记录对应一个实时订单事件。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| order_id | String | 订单唯一标识（UUID格式）|
| customer_id | String | 客户ID，格式 C{5位数字} |
| product_id | String | 商品ID，格式 P{6位数字} |
| product_category | String | 商品品类，见品类说明 |
| seller_id | String | 卖家ID，格式 S{4位数字} |
| price | Float64 | 商品售价（不含运费），单位：巴西雷亚尔 R$ |
| freight_value | Float64 | 运费，单位：R$ |
| order_status | String | 订单状态，见状态说明 |
| state | String | 客户所在州，两位缩写，如 SP / RJ |
| city | String | 客户所在城市 |
| event_time | DateTime | 事件发生时间（订单创建时间）|
| _ingest_time | DateTime | 数据入库时间（ClickHouse 写入时间）|
| _load_date | Date | 入库日期，用于分区 |

**TTL**：按 `_load_date` 分区，保留 24 小时。查询时建议加 `event_time >= now() - INTERVAL N MINUTE` 过滤。

---

### ods.payments_stream — 实时支付流

由 ClickHouse Kafka Engine 自动消费 `payments_stream` topic 写入。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| payment_id | String | 支付记录唯一标识（UUID）|
| order_id | String | 关联的订单ID |
| payment_type | String | 支付方式，见支付方式说明 |
| payment_value | Float64 | 实际支付金额（R$），含运费 |
| installments | UInt8 | 分期期数（1=不分期，最多12期）|
| event_time | DateTime | 支付事件时间 |
| _ingest_time | DateTime | 入库时间 |

**注意**：`payment_value` 是含运费的总支付金额，与 `ods.orders_stream.price` 含义不同，不要混用。

---

## DWD 层（实时明细宽表）

### dwd.realtime_order_detail — 实时订单+支付宽表

由 **Flink** 每分钟将 `ods.orders_stream` 与 `ods.payments_stream` JOIN 后写入。粒度：每条订单一行（含支付信息）。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| order_id | String | 订单ID |
| customer_id | String | 客户ID |
| product_id | String | 商品ID |
| product_category | String | 商品品类 |
| seller_id | String | 卖家ID |
| state | String | 客户所在州 |
| city | String | 客户所在城市 |
| price | Float64 | 商品价格（不含运费）|
| freight_value | Float64 | 运费 |
| total_amount | Float64 | price + freight_value，订单总金额 |
| payment_type | String | 支付方式（JOIN 自支付流，未支付时为空字符串）|
| payment_value | Float64 | 支付金额（未支付时为 0）|
| order_status | String | 订单状态 |
| event_time | DateTime | 订单事件时间 |
| event_date | Date | 订单日期（toDate(event_time)）|
| event_hour | UInt8 | 订单小时（0-23）|
| is_paid | UInt8 | 是否已支付：1=delivered状态，0=其他 |
| _ingest_time | DateTime | Flink 写入时间 |

**JOIN 延迟说明**：Flink 使用 Processing Time 关联，支付数据可能比订单数据晚几秒到几分钟到达，部分记录 `payment_type` 为空是正常现象。

---

## DWS 层（实时汇总层）

### dws.realtime_minute_stats — 分钟级聚合统计

由 **Flink 1分钟滚动窗口**计算后写入，每分钟产生一行聚合结果。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| window_start | DateTime | 窗口开始时间（整分钟）|
| window_end | DateTime | 窗口结束时间（window_start + 1分钟）|
| order_cnt | UInt64 | 该分钟内订单数 |
| total_gmv | Float64 | 该分钟内 GMV（sum(price)）|
| avg_price | Float64 | 该分钟内平均商品价格 |
| unique_customers | UInt64 | 该分钟内独立客户数 |
| top_category | String | 该分钟内订单量最多的品类 |
| _created_at | DateTime | 写入时间 |

**TTL**：保留 7 天。查询最新状态用 `ORDER BY window_start DESC LIMIT N`。

---

## ADS 层（实时应用视图）

ADS 层均为 ClickHouse **视图**，内置时间过滤，NL2SQL 可直接查询无需写时间条件。

### ads.realtime_hourly — 今日小时趋势

内置 `WHERE event_time >= today()` 过滤，每次查询返回当日实时数据。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| hour_start | DateTime | 小时起始时间（整点）|
| order_cnt | UInt64 | 该小时订单数 |
| gmv | Float64 | 该小时 GMV |
| avg_price | Float64 | 该小时平均价格 |
| unique_customers | UInt64 | 该小时独立客户数 |
| cancel_cnt | UInt64 | 该小时取消订单数 |

### ads.realtime_category_today — 今日品类排行

内置 `WHERE event_time >= today()`，按 gmv 降序排列。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| product_category | String | 商品品类 |
| order_cnt | UInt64 | 今日订单数 |
| gmv | Float64 | 今日 GMV |
| avg_price | Float64 | 今日平均价格 |

### ads.realtime_state_today — 今日州销售排行

内置 `WHERE event_time >= today()`，含排名字段。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| state | String | 州缩写 |
| order_cnt | UInt64 | 今日订单数 |
| gmv | Float64 | 今日 GMV |
| rank_by_gmv | UInt32 | 按 GMV 排名（1=最高）|

---

## Stream 层（流处理专用库）

### stream.ai_quality_alerts — AI 实时异常告警

由 Flink 异常检测模块写入，同时记录规则检测结果和 AI 分析建议。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| alert_id | String | 告警唯一ID（UUID）|
| alert_time | DateTime | 告警产生时间 |
| alert_type | String | 告警类型：ANOMALY（统计异常）/ QUALITY（数据质量）|
| severity | String | 严重程度：HIGH / MEDIUM / LOW |
| table_name | String | 告警来源表，通常为 ods.orders_stream |
| field_name | String | 触发告警的字段名 |
| detail | String | 告警详情描述（中文）|
| ai_suggestion | String | AI 给出的原因分析和处理建议 |
| window_start | DateTime | 触发告警的数据窗口开始时间 |
| window_end | DateTime | 触发告警的数据窗口结束时间 |
| metric_value | Float64 | 实际指标值 |
| threshold_value | Float64 | 触发告警的阈值 |

**TTL**：保留 30 天。
