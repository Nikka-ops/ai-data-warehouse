# 实时指标口径手册

## 核心交易指标

### GMV（Gross Merchandise Volume，商品成交总额）

**定义：** 所有订单的商品价格之和，不含运费，不扣除退款。
**计算公式：** GMV = SUM(price)，price 来自 ods.orders_stream 或 dwd.realtime_order_detail。
**注意事项：**
- GMV 包含所有状态订单（含取消），反映平台总交易规模
- GMV 不等于实际收入，实际收入需过滤 order_status = 'delivered'
- 运费（freight_value）不计入 GMV
- payment_value（支付流中的字段）含运费，不能替代 GMV 使用
- 单位：巴西雷亚尔（R$）

**实时查询示例：**
```sql
-- 最近10分钟 GMV
SELECT sum(price) AS gmv
FROM ods.orders_stream
WHERE event_time >= now() - INTERVAL 10 MINUTE

-- 今日分钟级 GMV 趋势（推荐，已预计算）
SELECT window_start, total_gmv
FROM dws.realtime_minute_stats
WHERE window_start >= today()
ORDER BY window_start

-- 今日 GMV 汇总（最简）
SELECT sum(gmv) AS today_gmv
FROM ads.realtime_hourly
```

---

### 实时 GMV vs 支付金额的区别

| 字段 | 来源表 | 含义 | 是否含运费 |
|------|--------|------|-----------|
| price | ods.orders_stream | 商品售价 | 否 |
| freight_value | ods.orders_stream | 运费 | — |
| total_amount | dwd.realtime_order_detail | price + freight_value | 是 |
| payment_value | ods.payments_stream | 用户实际支付金额 | 是 |
| total_gmv | dws.realtime_minute_stats | 分钟内 sum(price) | 否 |
| gmv | ads.realtime_hourly | 小时内 sum(price) | 否 |

**结论：** 计算 GMV 统一使用 sum(price)；计算用户实际支付用 sum(payment_value)。

---

### 客单价（Average Order Value，AOV）

**定义：** 平均每笔订单的商品金额。
**计算公式：** 客单价 = SUM(price) / COUNT(DISTINCT order_id)
**在数仓中：** dws.realtime_minute_stats.avg_price 为分钟内平均价格；ads.realtime_hourly.avg_price 为小时内平均价格。

**实时查询示例：**
```sql
-- 最近1小时平均客单价
SELECT avg(price) AS aov
FROM ods.orders_stream
WHERE event_time >= now() - INTERVAL 1 HOUR

-- 今日各小时平均客单价趋势
SELECT hour_start, avg_price FROM ads.realtime_hourly ORDER BY hour_start
```

---

### 独立客户数（Unique Customers）

**定义：** 去重后的下单客户数，衡量实时活跃买家规模。
**计算公式：** COUNT(DISTINCT customer_id)
**在数仓中：** dws.realtime_minute_stats.unique_customers 为分钟内去重客户数；ads.realtime_hourly.unique_customers 为小时内去重客户数。

**注意：** customer_id 在本实时系统中为每笔订单分配的唯一客户标识（格式 C{5位数字}），可直接用于去重计算活跃买家数。

---

### 订单量（Order Count）

**在数仓中：**
- dws.realtime_minute_stats.order_cnt：分钟级订单数
- ads.realtime_hourly.order_cnt：小时级订单数
- ads.realtime_category_today.order_cnt：今日各品类订单数

**实时查询示例：**
```sql
-- 最近5分钟每分钟订单量
SELECT window_start, order_cnt
FROM dws.realtime_minute_stats
WHERE window_start >= now() - INTERVAL 5 MINUTE
ORDER BY window_start

-- 今日各小时订单量
SELECT hour_start, order_cnt FROM ads.realtime_hourly ORDER BY hour_start
```

---

## 实时运营指标

### 分钟吞吐量（Throughput）

**定义：** 每分钟处理的订单数，衡量实时流量强度。
**来源：** dws.realtime_minute_stats.order_cnt
**异常阈值：** 当某分钟 order_cnt 偏离近期均值 ±2σ 时，Flink 触发 ANOMALY 告警。

**实时查询示例：**
```sql
-- 最近30分钟吞吐量趋势
SELECT window_start, order_cnt, total_gmv
FROM dws.realtime_minute_stats
WHERE window_start >= now() - INTERVAL 30 MINUTE
ORDER BY window_start
```

---

### 取消率（Cancellation Rate）

**定义：** 取消订单数占总订单数的百分比，衡量平台订单质量。
**计算公式：** 取消率 = COUNT(order_status='canceled') / COUNT(*) × 100%
**告警阈值：** Flink 规则引擎：当前分钟取消率 > 15% 触发 QUALITY 级别告警。

**实时查询示例：**
```sql
-- 今日各小时取消率
SELECT
    hour_start,
    order_cnt,
    cancel_cnt,
    round(cancel_cnt / order_cnt * 100, 2) AS cancel_rate_pct
FROM ads.realtime_hourly
ORDER BY hour_start

-- 最近10分钟取消率
SELECT
    countIf(order_status = 'canceled') AS cancel_cnt,
    count() AS total_cnt,
    round(countIf(order_status = 'canceled') / count() * 100, 2) AS cancel_rate_pct
FROM ods.orders_stream
WHERE event_time >= now() - INTERVAL 10 MINUTE
```

---

### 支付方式分布（Payment Type Distribution）

**定义：** 各支付方式的订单数和支付金额占比。
**字段：** ods.payments_stream.payment_type / dwd.realtime_order_detail.payment_type

**支付方式枚举值：**
- `credit_card`：信用卡（最常见）
- `boleto`：巴西银行转账单（boleto bancário）
- `voucher`：代金券
- `debit_card`：借记卡

**实时查询示例：**
```sql
-- 今日支付方式分布
SELECT
    payment_type,
    count() AS order_cnt,
    sum(payment_value) AS total_paid
FROM ods.payments_stream
WHERE event_time >= today()
GROUP BY payment_type
ORDER BY order_cnt DESC
```

---

### 分期付款分析（Installments）

**字段：** ods.payments_stream.installments（UInt8，1=不分期，最多12期）
**业务含义：** installments > 1 表示分期付款，巴西电商分期付款比例较高。

**实时查询示例：**
```sql
-- 今日分期 vs 全款比例
SELECT
    if(installments = 1, '全款', '分期') AS payment_mode,
    count() AS cnt
FROM ods.payments_stream
WHERE event_time >= today()
GROUP BY payment_mode
```

---

### 异常告警指标

**来源：** stream.ai_quality_alerts
**核心字段：**
- metric_value：实际指标值
- threshold_value：告警阈值
- severity：HIGH / MEDIUM / LOW
- alert_type：ANOMALY（流量异常）/ QUALITY（数据质量）

**Flink 内置告警规则：**

| 规则 | 触发条件 | 告警类型 | 严重级别 |
|------|---------|---------|---------|
| 订单量异常 | order_cnt 偏离均值 ±2σ | ANOMALY | HIGH |
| 高价格告警 | 单笔订单 price > R$3000 | QUALITY | MEDIUM |
| 高取消率 | 分钟取消率 > 15% | QUALITY | HIGH |

**查询示例：**
```sql
-- 最近1小时告警
SELECT alert_time, alert_type, severity, detail, ai_suggestion
FROM stream.ai_quality_alerts
WHERE alert_time >= now() - INTERVAL 1 HOUR
ORDER BY alert_time DESC

-- 今日 HIGH 级别告警数
SELECT count() AS high_alert_cnt
FROM stream.ai_quality_alerts
WHERE alert_time >= today() AND severity = 'HIGH'
```

---

## 品类与地域指标

### 今日品类 GMV 排行

**来源：** ads.realtime_category_today（内置 today() 过滤，直接查询）

```sql
SELECT product_category, order_cnt, gmv, avg_price
FROM ads.realtime_category_today
ORDER BY gmv DESC
LIMIT 10
```

### 今日各州销售排行

**来源：** ads.realtime_state_today（内置 today() 过滤，含 rank_by_gmv 排名字段）

```sql
SELECT state, order_cnt, gmv, rank_by_gmv
FROM ads.realtime_state_today
ORDER BY rank_by_gmv
LIMIT 10
```

---

## 数据新鲜度说明

| 数据层 | 数据延迟 | 说明 |
|--------|---------|------|
| ods.* | < 5 秒 | Kafka Engine 实时消费 |
| dwd.realtime_order_detail | < 1 分钟 | Flink Processing Time JOIN |
| dws.realtime_minute_stats | 1 分钟 | Flink 1分钟滚动窗口 |
| ads.* 视图 | < 1 分钟 | 基于 dwd 层实时视图 |
| stream.ai_quality_alerts | 1 分钟 | Flink 窗口结束后写入 |

**查询建议：** 需要最新数据优先查 ods 层；需要稳定聚合数据查 ads 视图或 dws 层。
