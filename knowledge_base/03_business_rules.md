# 业务规则手册

## 订单状态说明

本平台订单共有以下状态，存储在 order_status 字段中：

| 状态值 | 中文含义 | 说明 |
|--------|---------|------|
| created | 已创建 | 订单已创建但未付款 |
| approved | 已审核 | 支付已通过审核 |
| invoiced | 已开票 | 已生成发票，等待发货 |
| processing | 处理中 | 卖家正在准备商品 |
| shipped | 已发货 | 商品已交给物流 |
| delivered | 已送达 | 客户已收到商品（最终成功状态） |
| canceled | 已取消 | 订单已取消 |
| unavailable | 不可用 | 商品不可用，订单无法完成 |

**重要规则：**
- 计算实际成交金额时，只统计 delivered 状态
- 计算 GMV 时，包含所有状态（行业惯例）
- canceled 和 unavailable 是终态失败状态
- delivered 是终态成功状态

---

## 巴西州名对照表

state 字段为两位大写字母缩写，对应巴西各州：

| 缩写 | 州名（葡语） | 区域 | 经济特点 |
|------|------------|------|---------|
| SP | São Paulo | 东南部 | 巴西最大经济体，电商最发达 |
| RJ | Rio de Janeiro | 东南部 | 第二大城市，旅游+金融 |
| MG | Minas Gerais | 东南部 | 工业重镇 |
| RS | Rio Grande do Sul | 南部 | 农业+工业 |
| PR | Paraná | 南部 | 农业大州 |
| SC | Santa Catarina | 南部 | 制造业发达 |
| BA | Bahia | 东北部 | 东北部最大经济体 |
| GO | Goiás | 中西部 | 农业州 |
| DF | Distrito Federal | 中西部 | 首都巴西利亚所在地 |
| PE | Pernambuco | 东北部 | 东北部重要港口 |
| AM | Amazonas | 北部 | 亚马逊雨林，电商欠发达 |
| RO | Rondônia | 北部 | 偏远州，订单量少 |

**分析规律：** SP 州订单量通常占全国 40%+ ，东南部三州（SP+RJ+MG）合计占 60%+。

---

## 商品品类说明

product_category 字段经过标准化处理（下划线分隔），主要品类含义：

| 品类名 | 中文含义 | 特点 |
|--------|---------|------|
| Beleza_Saude | 美妆健康 | 销售额第一，高频消费品 |
| Relogios_Presentes | 手表礼品 | 客单价高，节日礼品 |
| Cama_Mesa_Banho | 床上用品 | 家居必需品，稳定需求 |
| Esporte_Lazer | 运动休闲 | 巴西体育文化浓厚 |
| Informatica_Acessorios | 电脑配件 | 数码配件，标品 |
| Moveis_Decoracao | 家具装饰 | 客单价高，件数少 |
| Utilidades_Domesticas | 家居用品 | 日常必需 |
| Ferramentas_Jardim | 工具园艺 | 季节性较强 |
| Automotivo | 汽车用品 | 巴西汽车普及率高 |
| Telefonia | 手机通讯 | 高客单价品类 |

---

## 时间规律说明

### 数据时间范围
- 完整数据：2017年1月 ~ 2018年8月
- 不完整月份：2016年（平台刚起步）、2018年9月（数据截断，仅145元GMV）
- 做趋势分析时建议过滤掉 2016年 和 2018-09

### 电商节日规律
- **11月黑色星期五（Black Friday）：** 巴西最大电商促销节，订单量峰值在此
- **圣诞节前（12月）：** 礼品类订单明显增加
- **情人节（2月）：** 手表、礼品类销售高峰
- **儿童节（10月12日）：** 玩具类销售高峰

### 已验证的业务规律
- 2017年11月24日（黑色星期五）单日订单量 1166 单，是日均(160单)的 7.3 倍
- 每年年初（1月）通常有一波增长（节后补货+新年促销）
- 2月因月份短且节后消费疲软，订单量通常环比下降

---

## 数仓层次使用指南

### 什么场景用哪张表？

| 分析需求 | 推荐使用的表 | 原因 |
|---------|------------|------|
| 月度GMV趋势 | ads.monthly_kpi | 已预计算，查询最快 |
| 日度GMV趋势 | dws.order_daily | 每日粒度，性能好 |
| 品类销售分析 | dws.category_daily | 已按品类聚合 |
| 地域销售排名 | ads.state_sales_rank | 已计算排名 |
| 用户行为分析 | dwd.order_detail | 最细粒度，字段最全 |
| 配送时效分析 | dwd.order_detail | delivery_days 字段 |
| 卖家分析 | dwd.order_detail | seller_id 字段 |

### 注意事项
- 不要在 dwd 层查 gmv 字段（不存在），应用 sum(price) 代替
- 统计用户数必须用 customer_unique_id，不能用 customer_id
- 查询月份格式用 '2018-01'，日期格式用 '2018-01-15'
- ClickHouse 时间函数与 MySQL 不同：用 toYear() 而非 YEAR()
