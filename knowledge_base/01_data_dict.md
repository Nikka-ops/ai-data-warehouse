# 数据字典

## 数据库总览

本数仓基于巴西电商平台 Olist 的真实交易数据，时间范围 2016年9月 ~ 2018年9月，共四个数据库层次：ods（原始层）、dwd（明细层）、dws（汇总层）、ads（应用层）。

---

## ODS 层（原始数据层）

### ods.orders_raw — 订单主表

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| order_id | String | 订单唯一标识，全局唯一 |
| customer_id | String | 客户ID，对应 customers 表 |
| order_status | String | 订单状态，见状态说明 |
| order_purchase_ts | DateTime | 下单时间 |
| order_approved_ts | DateTime | 支付审核通过时间 |
| order_delivered_ts | DateTime | 实际送达客户时间 |
| order_estimated_ts | DateTime | 预计送达时间 |

### ods.order_items_raw — 订单商品明细表

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| order_id | String | 订单ID，关联 orders 表 |
| order_item_id | UInt32 | 商品在订单内的序号（从1开始） |
| product_id | String | 商品ID |
| seller_id | String | 卖家ID |
| price | Float64 | 商品售价（不含运费），单位：巴西雷亚尔(R$) |
| freight_value | Float64 | 运费，单位：巴西雷亚尔(R$) |
| shipping_limit_ts | DateTime | 卖家最晚发货时间 |

### ods.customers_raw — 客户表

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| customer_id | String | 客户ID（每个订单生成一个，不是唯一用户标识） |
| customer_unique_id | String | 真实用户唯一标识，同一用户多次购买对应同一个 unique_id |
| city | String | 客户所在城市（葡萄牙语） |
| state | String | 客户所在州，两位缩写，如 SP=圣保罗、RJ=里约热内卢 |

### ods.products_raw — 商品表

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| product_id | String | 商品唯一标识 |
| product_category_name | String | 商品品类名称（葡萄牙语） |
| product_weight_g | Float64 | 商品重量（克） |
| product_length_cm | Float64 | 商品长度（厘米） |
| product_height_cm | Float64 | 商品高度（厘米） |
| product_width_cm | Float64 | 商品宽度（厘米） |

---

## DWD 层（明细数据层）

### dwd.order_detail — 订单明细宽表

这是数仓最核心的宽表，将订单、商品、客户、商品四张表关联后生成，粒度为"订单商品行"（一条记录 = 一个订单里的一件商品）。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| order_id | String | 订单ID |
| order_item_id | UInt32 | 商品序号 |
| customer_id | String | 客户ID |
| customer_unique_id | String | 用户唯一ID（去重计算用户数时用此字段） |
| city | String | 客户城市 |
| state | String | 客户所在州 |
| product_id | String | 商品ID |
| product_category | String | 商品品类（已标准化，下划线分隔） |
| seller_id | String | 卖家ID |
| order_status | String | 订单状态 |
| price | Float64 | 商品价格（GMV的计算基础） |
| freight_value | Float64 | 运费 |
| total_amount | Float64 | price + freight_value，订单总金额 |
| order_date | Date | 下单日期 |
| order_year | UInt16 | 下单年份 |
| order_month | UInt8 | 下单月份（1-12） |
| order_hour | UInt8 | 下单小时（0-23） |
| delivery_days | Int32 | 配送天数（下单到送达的自然日数） |
| is_delivered | UInt8 | 是否已送达：1=已送达，0=未送达 |

---

## DWS 层（汇总数据层）

### dws.order_daily — 每日订单汇总表

粒度：每天一行。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| dt | Date | 统计日期 |
| order_cnt | UInt64 | 当日订单数（去重） |
| item_cnt | UInt64 | 当日商品件数 |
| gmv | Float64 | 当日GMV（商品成交额，不含运费） |
| freight_total | Float64 | 当日运费总额 |
| user_cnt | UInt64 | 当日下单用户数（按customer_unique_id去重） |
| delivered_cnt | UInt64 | 当日已送达订单数 |
| cancel_cnt | UInt64 | 当日取消订单数 |
| avg_order_value | Float64 | 当日客单价 = gmv / order_cnt |

### dws.category_daily — 品类每日汇总表

粒度：每天每个品类一行。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| dt | Date | 统计日期 |
| product_category | String | 商品品类名称 |
| order_cnt | UInt64 | 该品类当日订单数 |
| gmv | Float64 | 该品类当日GMV |
| avg_price | Float64 | 该品类当日平均售价 |

---

## ADS 层（应用数据层）

### ads.monthly_kpi — 月度核心KPI表

粒度：每月一行，直接面向报表展示。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| ym | String | 年月，格式：2018-01 |
| gmv | Float64 | 月度GMV |
| order_cnt | UInt64 | 月度订单数 |
| user_cnt | UInt64 | 月度活跃用户数 |
| avg_order_value | Float64 | 月度客单价 |
| mom_gmv_rate | Float64 | GMV环比增长率（%），第一个月为NULL |

### ads.state_sales_rank — 省份销售排行表

粒度：每月每个州一行。

| 字段名 | 类型 | 业务含义 |
|--------|------|---------|
| dt_month | String | 年月，格式：2018-01 |
| state | String | 州名缩写 |
| gmv | Float64 | 该州当月GMV |
| order_cnt | UInt64 | 该州当月订单数 |
| rank_by_gmv | UInt32 | 按GMV排名（1=最高） |
