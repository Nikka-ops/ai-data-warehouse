# 指标口径手册

## 核心交易指标

### GMV（Gross Merchandise Volume，商品成交总额）

**定义：** 所有订单的商品价格之和，不含运费，不扣除退款。
**计算公式：** GMV = SUM(price)，其中 price 来自 dwd.order_detail 或 dws 层的 gmv 字段。
**注意事项：**
- GMV 包含已取消订单，反映平台总交易规模
- GMV 不等于实际收入，实际收入需扣除退款、佣金等
- 运费（freight_value）不计入 GMV
- 本数仓 GMV 单位为巴西雷亚尔（R$）

**示例 SQL：**
```sql
SELECT sum(price) AS gmv FROM dwd.order_detail
```

---

### 销售额 vs GMV 的区别

- **GMV**：包含所有状态的订单（含取消、退款），反映平台交易规模
- **实际销售额**：只计算 delivered（已送达）订单，反映真实成交
- 在本数仓中，dws/ads 层的 gmv 字段 = 所有状态订单的 price 之和（即 GMV 口径）
- 若需要"实际销售额"，需过滤 order_status = 'delivered'

---

### 客单价（Average Order Value，AOV）

**定义：** 平均每笔订单的商品金额。
**计算公式：** 客单价 = GMV / 订单数 = SUM(price) / COUNT(DISTINCT order_id)
**在数仓中：** dws.order_daily 和 ads.monthly_kpi 中的 avg_order_value 字段已预计算。
**本数据集客单价：** 约 R$ 132.71（约合人民币 180 元）

---

### 订单数 vs 商品件数

- **订单数（order_cnt）：** 按 order_id 去重计数，一个订单可包含多件商品
- **商品件数（item_cnt）：** 订单商品明细行数，不去重
- 两者关系：item_cnt >= order_cnt，差值为多件商品订单的额外件数
- 本数据集：订单数 98,666，商品件数 112,650，平均每单 1.14 件

---

### 用户数 vs 客户数

- **customer_id：** 每笔订单生成一个，同一用户多次购买有多个 customer_id
- **customer_unique_id：** 真实用户唯一标识，计算 UV（独立用户数）时必须用此字段
- **用户数（user_cnt）：** COUNT(DISTINCT customer_unique_id)
- 本数据集：订单数 98,666，独立用户数 97,729，说明绝大多数用户只买过一次（复购率极低）

---

### 环比增长率（Month-over-Month，MoM）

**定义：** 本月指标相比上月的增长幅度。
**计算公式：** MoM = (本月值 - 上月值) / 上月值 × 100%
**在数仓中：** ads.monthly_kpi 表中的 mom_gmv_rate 字段，单位为百分比（%）
**示例：** mom_gmv_rate = 27.71 表示本月GMV比上月增长了 27.71%

---

### 同比增长率（Year-over-Year，YoY）

**定义：** 本月指标相比去年同月的增长幅度。
**注意：** 本数仓 ADS 层未预计算同比，需要在查询时自行计算：
```sql
-- 计算同比示例
SELECT
    a.ym,
    a.gmv AS 本月GMV,
    b.gmv AS 去年同月GMV,
    round((a.gmv - b.gmv) / b.gmv * 100, 2) AS yoy_rate
FROM ads.monthly_kpi a
LEFT JOIN ads.monthly_kpi b
    ON substring(a.ym, 6, 2) = substring(b.ym, 6, 2)  -- 月份相同
    AND toInt32(substring(a.ym, 1, 4)) = toInt32(substring(b.ym, 1, 4)) + 1  -- 年份差1
```

---

### 配送时效（Delivery Days）

**定义：** 从客户下单到实际收货的自然日数。
**计算公式：** delivery_days = dateDiff('day', order_purchase_ts, order_delivered_ts)
**在数仓中：** dwd.order_detail 表的 delivery_days 字段（仅 delivered 状态订单有值）
**本数据集平均配送时效：** 约 12 天（巴西地域广阔，配送时间较长）

---

## 运营分析指标

### 取消率（Cancellation Rate）

**计算公式：** 取消率 = 取消订单数 / 总订单数 × 100%
**本数据集取消率：** 542 / 98,666 ≈ 0.55%，取消率极低

---

### 送达率（Delivery Rate）

**计算公式：** 送达率 = 已送达订单数 / 总订单数 × 100%
**本数据集送达率：** 97.8%，物流体系成熟

---

### 复购率（Repurchase Rate）

**定义：** 购买过2次及以上的用户占总用户数的比例。
**本数据集：** 订单数(98,666) ≈ 用户数(97,729)，说明复购率接近0，几乎每个用户只买过一次。
**分析：** 这是新兴电商平台的典型特征，用户获取快但留存差，需要重点投入复购运营。
