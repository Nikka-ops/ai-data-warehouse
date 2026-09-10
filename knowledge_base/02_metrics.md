# 指标口径手册

金额单位统一为人民币（￥）。

---

## 核心交易指标

### GMV（商品成交总额）

**定义：** 订单明细分摊后的成交额之和，不含运费，不扣除退款。

**在哪查：** `dws.trade_window_agg.order_amount`。注意字段名是 `order_amount`，
不叫 `gmv` —— 这是最常写错的一个名字。

**两种口径不要混：**

```sql
-- 今日累计 GMV：走 CUMULATE_1D 的最新一行，一次点查
SELECT order_amount FROM dws.trade_window_agg
WHERE stat_date = CURDATE() AND window_type = 'CUMULATE_1D'
  AND category1_name = 'ALL' AND region_name = 'ALL'
ORDER BY window_start DESC LIMIT 1;

-- 某个时间段的 GMV：走 TUMBLE_1M 分钟窗口再 SUM
SELECT SUM(order_amount) FROM dws.trade_window_agg
WHERE window_type = 'TUMBLE_1M'
  AND category1_name = 'ALL' AND region_name = 'ALL'
  AND window_start >= DATE_SUB(NOW(), INTERVAL 30 MINUTE);
```

**不要把 CUMULATE_1D 的多行 SUM 起来** —— 累计窗口每分钟输出一行「截至此刻的累计值」，
相加会得到一个毫无意义的数。

---

### 实付金额 vs GMV

- **GMV**：所有已下单商品的成交额，包含未支付和已取消的订单，反映交易规模
- **实付金额**：`dws.pay_window_agg.pay_amount`，只统计支付成功的回调，反映真实收款

两者不会相等，差额主要是未支付和已取消的部分。

---

### 客单价 / 件均价

- **客单价** = `order_amount / order_cnt`，一个订单平均多少钱
- **件均价** = `order_amount / sku_num`，一件商品平均多少钱

两者不是一回事：一单买三件时，客单价是件均价的三倍。
质检器检测的「均价异常」用的是件均价 —— 它对商品配错价格更敏感。

---

### 支付转化率

**定义：** 状态已经推进到支付及之后的订单 / 总订单数。

```sql
SELECT pay_conversion_pct FROM ads.v_today_conversion;
```

**只能从 `dwd.order_snapshot` 算。** 这个指标问的是「有多少订单当前处于已支付状态」，
是状态分布，不是事件计数。窗口聚合表装的是「发生了多少次支付」，
回答不了「现在有多少订单是已支付的」。

---

## 用户指标

### 独立用户数（UV）

**必须走 BITMAP。** 这是本数仓最容易算错的指标。

```sql
-- 正确：跨天精确去重
SELECT BITMAP_UNION_COUNT(uv_bitmap) FROM dws.user_active_daily
WHERE region_name = 'ALL' AND dt >= CURDATE() - INTERVAL 6 DAY;
```

**三种典型错误写法：**

| 错误写法 | 为什么错 |
|---------|---------|
| `SUM(order_user_cnt)` | 那是**单个分钟窗口内**的去重用户数。同一个用户在 10:01 和 10:05 各下一单，两个窗口各记一次，相加得 2 |
| `COUNT(uv_bitmap)` / `SUM(uv_bitmap)` | BITMAP 是位图类型，不是数字，这两种写法要么报错要么返回无意义的值 |
| 把每天的 UV 数字加起来 | 跨天重复活跃的用户被重复计数 |

**为什么用 BITMAP 而不是 `COUNT(DISTINCT user_id)`：**
后者每查一次就要扫一次全量明细，问「最近 30 天有多少独立用户」就得扫 30 个分区的
全部订单。BITMAP 把「当天有哪些用户」这个集合本身存下来，跨天查询变成压缩位图上的
或运算，不回明细，且结果是精确值（不是 HLL 那种估算）。

---

## 链路时效指标

### 端到端延迟（lag_seconds）

**定义：** 事件在业务库发生（`create_time` / `callback_time`）→ Flink 加工完成，
中间经过的秒数。在 DWD 层按单条记录计算，DWS 层取 `MAX` 和 `AVG`。

**为什么两个都要看：** 只看 AVG 会把偶发的严重延迟平均掉；只看 MAX 又容易被单点噪声带偏。
两个一起看才判断得出来是链路整体慢了，还是个别数据卡住了。

正常水位在个位数秒。超过 120 秒质检器会报 `LATENCY` 告警，
排查方向是 Flink 背压、Kafka 消费积压、Doris 导入变慢。

### 迟到率

**定义：** 落到 `stream.late_records` 的记录数 / 同期订单数。

watermark 给了 5 分钟乱序容忍，这个范围内的乱序数据能正常进窗口参与聚合。
超出 5 分钟的进不了原窗口 —— 窗口 TVF 会直接丢弃且不留痕迹，
所以 DWS 作业用 `CURRENT_WATERMARK()` 把这批记录分流到兜底表。

迟到率抬头说明窗口结果的完整性在下降，通常是上游延迟或 watermark 容忍度设得不合适。
这些数据没有丢，带着 Kafka 位点，可以精确回放补算。
