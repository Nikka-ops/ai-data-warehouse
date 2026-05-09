-- ============================================================
-- AI 数仓初始化脚本
-- 执行顺序：Docker 启动时自动执行
-- ============================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dws;
CREATE DATABASE IF NOT EXISTS ads;

-- ============================================================
-- ODS 层：原始数据（直接贴近源数据，不做业务加工）
-- ============================================================

-- 原始订单表
CREATE TABLE IF NOT EXISTS ods.orders_raw (
    order_id            String,
    customer_id         String,
    order_status        String,         -- pending / processing / shipped / delivered / canceled
    order_purchase_ts   DateTime,
    order_approved_ts   Nullable(DateTime),
    order_delivered_ts  Nullable(DateTime),
    order_estimated_ts  Nullable(DateTime),
    _load_time          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY toYYYYMM(order_purchase_ts)
ORDER BY (order_id)
COMMENT '原始订单表 - ODS层';

-- 原始订单商品表
CREATE TABLE IF NOT EXISTS ods.order_items_raw (
    order_id            String,
    order_item_id       UInt32,
    product_id          String,
    seller_id           String,
    price               Float64,
    freight_value       Float64,
    shipping_limit_ts   Nullable(DateTime),
    _load_time          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY tuple()
ORDER BY (order_id, order_item_id)
COMMENT '原始订单商品明细表 - ODS层';

-- 原始客户表
CREATE TABLE IF NOT EXISTS ods.customers_raw (
    customer_id         String,
    customer_unique_id  String,
    city                String,
    state               String,
    _load_time          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY tuple()
ORDER BY (customer_id)
COMMENT '原始客户表 - ODS层';

-- 原始商品分类表
CREATE TABLE IF NOT EXISTS ods.products_raw (
    product_id              String,
    product_category_name   String,
    product_weight_g        Nullable(Float64),
    product_length_cm       Nullable(Float64),
    product_height_cm       Nullable(Float64),
    product_width_cm        Nullable(Float64),
    _load_time              DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY tuple()
ORDER BY (product_id)
COMMENT '原始商品表 - ODS层';

-- ============================================================
-- DWD 层：明细数据（清洗、标准化、关联后的宽表）
-- ============================================================

CREATE TABLE IF NOT EXISTS dwd.order_detail (
    order_id            String,
    order_item_id       UInt32,
    customer_id         String,
    customer_unique_id  String,
    city                String,
    state               String,
    product_id          String,
    product_category    String,
    seller_id           String,
    order_status        String,
    -- 金额
    price               Float64,
    freight_value       Float64,
    total_amount        Float64,        -- price + freight_value
    -- 时间
    order_date          Date,
    order_year          UInt16,
    order_month         UInt8,
    order_hour          UInt8,
    -- 派生字段
    delivery_days       Nullable(Int32), -- 下单到送达天数
    is_delivered        UInt8,           -- 1=已送达
    _load_time          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_id, order_item_id)
COMMENT '订单明细宽表 - DWD层';

-- ============================================================
-- DWS 层：汇总数据（按主题聚合的宽表）
-- ============================================================

-- 每日销售汇总
CREATE TABLE IF NOT EXISTS dws.order_daily (
    dt              Date,
    order_cnt       UInt64,         -- 订单数
    item_cnt        UInt64,         -- 商品件数
    gmv             Float64,        -- 商品成交额
    freight_total   Float64,        -- 运费总额
    user_cnt        UInt64,         -- 下单用户数（unique）
    delivered_cnt   UInt64,         -- 已送达订单数
    cancel_cnt      UInt64,         -- 取消订单数
    avg_order_value Float64,        -- 客单价
    _load_time      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY toYYYYMM(dt)
ORDER BY (dt)
COMMENT '每日销售汇总 - DWS层';

-- 品类销售汇总
CREATE TABLE IF NOT EXISTS dws.category_daily (
    dt                  Date,
    product_category    String,
    order_cnt           UInt64,
    gmv                 Float64,
    avg_price           Float64,
    _load_time          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, product_category)
COMMENT '品类每日汇总 - DWS层';

-- ============================================================
-- ADS 层：应用数据（直接面向看板和报表）
-- ============================================================

-- 核心 KPI 指标（月度）
CREATE TABLE IF NOT EXISTS ads.monthly_kpi (
    ym              String,         -- 格式：2018-01
    gmv             Float64,
    order_cnt       UInt64,
    user_cnt        UInt64,
    avg_order_value Float64,
    mom_gmv_rate    Nullable(Float64),  -- 环比增长率
    _load_time      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY tuple()
ORDER BY (ym)
COMMENT '月度核心KPI - ADS层';

-- 省份销售排行
CREATE TABLE IF NOT EXISTS ads.state_sales_rank (
    dt_month        String,
    state           String,
    gmv             Float64,
    order_cnt       UInt64,
    rank_by_gmv     UInt32,
    _load_time      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_load_time)
PARTITION BY tuple()
ORDER BY (dt_month, rank_by_gmv)
COMMENT '省份销售排行 - ADS层';
