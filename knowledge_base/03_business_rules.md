# 实时业务规则手册

## 订单状态说明

order_status 字段枚举值，存在于 ods.orders_stream 和 dwd.realtime_order_detail：

| 状态值 | 中文含义 | 说明 |
|--------|---------|------|
| created | 已创建 | 订单已创建但未付款 |
| approved | 已审核 | 支付已通过审核 |
| invoiced | 已开票 | 已生成发票，等待发货 |
| processing | 处理中 | 卖家正在准备商品 |
| shipped | 已发货 | 商品已交给物流 |
| delivered | 已送达 | 客户已收到商品（终态成功）|
| canceled | 已取消 | 订单已取消（终态失败）|
| unavailable | 不可用 | 商品不可用（终态失败）|

**重要规则：**
- 计算实际成交金额只统计 `delivered` 状态
- 计算 GMV 包含所有状态（行业惯例）
- `canceled` 和 `unavailable` 是终态失败状态
- `is_paid` 字段（dwd 层）：delivered 状态 = 1，其他 = 0

---

## 商品品类说明

product_category 字段为标准化字符串（下划线分隔），主要品类：

| 品类名 | 中文含义 |
|--------|---------|
| beleza_saude | 美妆健康 |
| relogios_presentes | 手表礼品 |
| cama_mesa_banho | 床上用品 |
| esporte_lazer | 运动休闲 |
| informatica_acessorios | 电脑配件 |
| moveis_decoracao | 家具装饰 |
| utilidades_domesticas | 家居用品 |
| ferramentas_jardim | 工具园艺 |
| automotivo | 汽车用品 |
| telefonia | 手机通讯 |

**dws.realtime_minute_stats.top_category**：该字段记录每分钟内订单量最多的品类名称。

---

## 巴西州名说明

state 字段为两位大写字母缩写，对应巴西各州：

| 缩写 | 州名 | 区域 |
|------|------|------|
| SP | São Paulo | 东南部（最大电商市场）|
| RJ | Rio de Janeiro | 东南部 |
| MG | Minas Gerais | 东南部 |
| RS | Rio Grande do Sul | 南部 |
| PR | Paraná | 南部 |
| SC | Santa Catarina | 南部 |
| BA | Bahia | 东北部 |
| GO | Goiás | 中西部 |
| DF | Distrito Federal | 中西部（首都）|
| PE | Pernambuco | 东北部 |
| AM | Amazonas | 北部 |

**分析规律：** SP 州订单量通常占全国 40%+，东南部三州（SP+RJ+MG）合计占 60%+。ads.realtime_state_today 含 rank_by_gmv 排名字段，可直接排名查询。

---

## Flink 窗口语义

### 1分钟滚动窗口（Tumbling Window）

- 窗口边界：整分钟对齐，如 12:00:00 ~ 12:01:00
- window_start：窗口开始时间（含）
- window_end：window_start + 1分钟（不含）
- 写入时机：窗口结束后 Flink 触发计算并写入 dws.realtime_minute_stats
- 数据延迟：窗口结束后约 1-5 秒完成写入

**查询最新窗口：**
```sql
SELECT * FROM dws.realtime_minute_stats
ORDER BY window_start DESC LIMIT 1
```

### Processing Time vs Event Time

本系统 Flink 作业使用 **Processing Time**（处理时间）而非 Event Time：
- 优点：实现简单，无需 Watermark 配置
- 注意：若消息有延迟（如网络抖动），可能归入下一个窗口
- 支付流 JOIN 延迟：支付消息比订单消息晚几秒到几分钟到达，dwd 层部分记录 payment_type 为空是正常现象

---

## 实时异常检测规则

Flink 作业内置规则引擎（毫秒级响应，无需 LLM）：

### 规则1：订单量统计异常

- **检测方法：** ±2σ 算法（基于近30分钟滑动均值）
- **触发条件：** 当前分钟 order_cnt 超出 [均值 - 2σ, 均值 + 2σ] 范围
- **告警类型：** ANOMALY
- **严重级别：** HIGH
- **字段：** metric_value = 当前 order_cnt，threshold_value = 均值 ± 2σ 边界

### 规则2：高价格异常

- **触发条件：** 单笔订单 price > R$3000
- **告警类型：** QUALITY
- **严重级别：** MEDIUM
- **字段：** field_name = 'price'，metric_value = 实际价格，threshold_value = 3000

### 规则3：高取消率

- **触发条件：** 当前1分钟窗口内取消率 > 15%
- **告警类型：** QUALITY
- **严重级别：** HIGH
- **计算：** cancel_cnt / order_cnt > 0.15

**AI 分析触发时机：** 仅当规则引擎触发告警后，才调用 DeepSeek 生成 ai_suggestion，节省推理成本。

---

## 时间查询规则

### 实时时间过滤模式

| 查询意图 | 推荐写法 | 说明 |
|---------|---------|------|
| 今天 / 今日 | `event_time >= today()` | ClickHouse today() 返回当日0点 |
| 最近N分钟 | `event_time >= now() - INTERVAL N MINUTE` | now() 为当前时间 |
| 最近N小时 | `event_time >= now() - INTERVAL N HOUR` | 跨天时优于 today() |
| 本小时 | `event_time >= toStartOfHour(now())` | 精确到整点 |
| 最新N条 | `ORDER BY event_time DESC LIMIT N` | 无需时间过滤 |

### ADS 视图无需写时间条件

ads.* 层视图已内置 `WHERE event_time >= today()` 过滤，查询时**不需要**再加时间条件：
```sql
-- 正确：直接查，视图已有时间过滤
SELECT * FROM ads.realtime_category_today ORDER BY gmv DESC

-- 错误：多余的时间过滤（不会报错但语义重复）
SELECT * FROM ads.realtime_category_today WHERE event_time >= today()
```

### ODS 层查询必须加时间过滤

ods 层保留 24 小时数据，**必须加时间过滤**，否则扫描全量数据：
```sql
-- 正确
SELECT count() FROM ods.orders_stream WHERE event_time >= now() - INTERVAL 10 MINUTE

-- 错误：扫描全天数据，性能差
SELECT count() FROM ods.orders_stream
```

---

## 数仓层次使用指南

### 实时查询场景 → 推荐表

| 分析需求 | 推荐表 | 原因 |
|---------|--------|------|
| 今日小时趋势（GMV/订单量/取消数）| ads.realtime_hourly | 已预聚合，内置 today() 过滤 |
| 今日品类 GMV 排行 | ads.realtime_category_today | 已预聚合，直接查询 |
| 今日各州销售排名 | ads.realtime_state_today | 含 rank_by_gmv 排名字段 |
| 分钟级流量趋势 | dws.realtime_minute_stats | 1分钟粒度，最近7天 |
| 最新N分钟原始订单明细 | ods.orders_stream + 时间过滤 | 最细粒度，需加时间条件 |
| 订单+支付关联分析 | dwd.realtime_order_detail | 已 JOIN，含 payment_type |
| 异常告警查询 | stream.ai_quality_alerts | 含 AI 分析建议 |

### 不同时间粒度 → 推荐查询方式

| 时间粒度 | 推荐方式 |
|---------|---------|
| 秒级 / 最近几秒 | 直接查 ods 层（加 LIMIT） |
| 分钟级 | dws.realtime_minute_stats |
| 小时级 / 今日 | ads.realtime_hourly |
| 品类/地域维度 | ads.realtime_category_today / ads.realtime_state_today |

---

## 数据 ID 格式说明

| 字段 | 格式 | 示例 |
|------|------|------|
| order_id | UUID（36位） | `550e8400-e29b-41d4-a716-446655440000` |
| payment_id | UUID（36位） | `550e8400-e29b-41d4-a716-446655440001` |
| customer_id | C + 5位数字 | `C12345` |
| product_id | P + 6位数字 | `P123456` |
| seller_id | S + 4位数字 | `S1234` |
| alert_id | UUID（36位） | — |

---

## 常见查询错误与正确写法

### 1. 混用 price 和 payment_value

```sql
-- 错误：payment_value 含运费，不能用于 GMV
SELECT sum(payment_value) AS gmv FROM ods.payments_stream WHERE event_time >= today()

-- 正确：GMV 使用 price
SELECT sum(price) AS gmv FROM ods.orders_stream WHERE event_time >= today()
```

### 2. 遗漏时间过滤导致全表扫描

```sql
-- 错误：ods 层无时间过滤
SELECT avg(price) FROM ods.orders_stream

-- 正确：加时间范围
SELECT avg(price) FROM ods.orders_stream WHERE event_time >= now() - INTERVAL 1 HOUR
```

### 3. 对 ADS 视图加重复时间过滤

```sql
-- 可接受但冗余
SELECT * FROM ads.realtime_hourly WHERE event_time >= today()

-- 更简洁
SELECT * FROM ads.realtime_hourly
```

### 4. 统计独立客户数用错字段

```sql
-- 正确：customer_id 在本实时系统中已是订单级唯一客户标识
SELECT count(DISTINCT customer_id) AS unique_customers
FROM ods.orders_stream WHERE event_time >= today()

-- 或使用预计算字段
SELECT sum(unique_customers) FROM ads.realtime_hourly
```
