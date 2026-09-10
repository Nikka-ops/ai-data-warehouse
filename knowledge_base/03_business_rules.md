# 业务规则手册

## 订单状态机

订单状态存储在 `dwd.order_snapshot.order_status`，**全大写**：

| 状态值 | 中文含义 | 说明 |
|--------|---------|------|
| CREATED | 已下单 | 订单已创建，尚未支付 |
| PAID | 已支付 | 支付回调成功 |
| SHIPPED | 已发货 | 商品已交给物流 |
| DELIVERED | 已送达 | 终态成功 |
| CANCELED | 已取消 | 终态失败，可能发生在支付前或支付后 |
| REFUNDED | 已退款 | 终态失败，发生在支付之后 |

流转路径：

```
CREATED ──85%──> PAID ──97%──> SHIPPED ──> DELIVERED
   │                 │
   └──15%──> CANCELED└──3%──> REFUNDED
```

**重要规则：**
- 每一次状态流转都是业务库上的一条 UPDATE，CDC 捕获到的是同一主键的多条变更
- 判断「已支付」要包含 `PAID`、`SHIPPED`、`DELIVERED` 三个状态，
  只查 `PAID` 会漏掉已经发货和送达的订单
- 快照表里读到的一定是当前状态：Unique Key 按主键覆盖，
  `update_time` 作为 sequence 列保证迟到的旧状态盖不掉新状态

---

## 数据一致性是怎么保证的

三件事叠在一起，缺一不可：

**1. Flink 两阶段提交（写入端）**
Doris sink 开启 `sink.enable-2pc`，Stream Load 的事务与 Flink checkpoint 对齐。
checkpoint 成功才提交，失败则整批回滚。

**2. Doris Unique Key（Sink 端幂等）**
作业从 checkpoint 恢复时会重放一段数据。Unique Key 按主键覆盖，
重放多少次结果都一样 —— 这一环不成立的话，两阶段提交也保证不了端到端一致。

**3. Sequence Column（乱序容忍）**
binlog 事件经过 Kafka 多分区、Flink 多并行度之后，到达顺序不再保证。
没有 sequence column 时是「后写入的赢」，一条迟到的 CREATED 能把已经是
DELIVERED 的订单改回去。指定 `update_time` 为 sequence 列后变成
「事件时间大的赢」，旧状态永远盖不掉新状态。

**这三条合起来的效果**：同一份数据无论被处理几次、以什么顺序到达，
最终都收敛到同一个正确值，不会重复计算，也不会被旧状态覆盖。

**怎么验证**：`pipelines/reconcile.py` 定时拿 MySQL 和 Doris 的同口径数字比一遍，
差异落 `ads.reconcile_result`。设计上的保证需要有人持续检验。

---

## 窗口口径规则（查询时最容易踩的坑）

### 规则一：window_type 必须显式过滤

`dws.trade_window_agg` 一张表装了两种窗口。不加 `window_type` 过滤，
`TUMBLE_1M` 的分钟明细和 `CUMULATE_1D` 的累计值会被混在一起，结果毫无意义。

### 规则二：两个维度列必须同时约束

Flink 用 GROUPING SETS 一次算出「整体 / 分品类 / 分大区」三个粒度，
靠 `category1_name` / `region_name` 取值 `'ALL'` 区分汇总行与明细行。

```sql
-- 查整体
WHERE category1_name = 'ALL' AND region_name = 'ALL'
-- 按品类下钻
WHERE category1_name <> 'ALL' AND region_name = 'ALL'
-- 按大区下钻
WHERE region_name <> 'ALL' AND category1_name = 'ALL'
```

只约束一个维度，另一个维度的汇总行会和明细行叠加，**指标翻倍且不报错** ——
这是最难发现的一类错误。`dws.pay_window_agg` 同理，维度列是 `payment_type`。

### 规则三：跨窗口的用户数不能相加

见指标手册的 UV 章节。`order_user_cnt` 只在单个窗口内有效。

---

## 事务事实表 vs 累积快照

实时数仓建模里最需要分清的一组概念，本项目的分层直接建立在它上面。

| | 事务事实表 | 累积快照 |
|---|---|---|
| 记录什么 | 一次业务动作 | 一个业务实体的当前状态 |
| 本项目对应 | Kafka 的 `dwd_trade_order` / `dwd_trade_pay` | `dwd.order_snapshot` |
| 数据来源 | insert-only 的业务表（订单明细、支付流水） | 会反复 UPDATE 的订单主表 |
| 流的性质 | append-only | changelog（带撤回语义） |
| 能不能做窗口聚合 | 能 | **不能** |
| 回答什么问题 | 发生了多少次、多少钱 | 现在有多少处于某状态 |

**为什么 changelog 不能做窗口聚合：** Flink 的窗口 TVF（TUMBLE / CUMULATE）
只接受 append-only 流，把 CDC 的 changelog 直接喂进去会报
`doesn't support consuming update changes`。这不是一个可以绕过的报错，
而是在提醒建模分错了 —— 「今天成交了多少钱」算的是事件，
不该拿「当前有多少订单处于某状态」的表去算。

所以 GMV、支付额走窗口聚合，取消率、支付转化率走累积快照。

---

## 异常价格判定

`is_price_abnormal` 在 DWD 层就打好标记：成交价高于商品标价 1.5 倍
或低于 0.3 倍，即判为异常。

这类情况通常意味着商品配置错价，或者有人在薅羊毛。
DWS 层汇总成 `abnormal_price_cnt`，占比超过 5% 时质检器报 `QUALITY` 告警。
