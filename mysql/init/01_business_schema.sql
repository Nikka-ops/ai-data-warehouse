-- ============================================================
-- 业务库 schema（模拟电商交易系统的 OLTP 库）
--
-- 这是整条实时链路的真正源头。刻意按业务库的方式建，而不是按数仓：
--   * 表名是业务语义（order_info / payment_info），不带 ods_ 前缀
--   * 有主键、有索引、有 create_time / update_time
--   * 订单状态是一个会被反复 UPDATE 的字段
--
-- 最后一条是关键。真实电商里一笔订单的生命周期是
--   下单 → 支付 → 发货 → 送达（或中途取消 / 退款）
-- 每一次流转都是业务库上的一条 UPDATE，Flink CDC 捕获到的是
-- 同一主键的多条变更记录。这才是下游需要「按主键幂等 + 事件时间防乱序」
-- 的真实原因 —— 如果每笔订单只 INSERT 一次、状态不再变，
-- 那些设计就是摆设。
-- ============================================================

SET NAMES utf8mb4;

CREATE DATABASE IF NOT EXISTS mall
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE mall;


-- ============================================================
-- 订单主表 —— 状态会随生命周期反复更新
-- ============================================================
CREATE TABLE IF NOT EXISTS order_info (
    id                  BIGINT          NOT NULL AUTO_INCREMENT COMMENT '订单ID',
    user_id             BIGINT          NOT NULL                COMMENT '用户ID',
    province_id         INT             NOT NULL                COMMENT '省份ID',

    order_status        VARCHAR(16)     NOT NULL                COMMENT '订单状态：CREATED/PAID/SHIPPED/DELIVERED/CANCELED/REFUNDED',
    total_amount        DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '订单总额（含运费）',
    activity_reduce     DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '活动优惠金额',
    coupon_reduce       DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '优惠券优惠金额',
    freight_amount      DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '运费',

    create_time         DATETIME(3)     NOT NULL                COMMENT '下单时间（业务事件时间）',
    payment_time        DATETIME(3)     NULL                    COMMENT '支付时间',
    ship_time           DATETIME(3)     NULL                    COMMENT '发货时间',
    receive_time        DATETIME(3)     NULL                    COMMENT '确认收货时间',
    cancel_time         DATETIME(3)     NULL                    COMMENT '取消时间',
    -- CDC 侧用它判断变更先后，是下游 Sequence Column 的取值来源
    update_time         DATETIME(3)     NOT NULL                COMMENT '最后更新时间',

    PRIMARY KEY (id),
    KEY idx_update_time (update_time),
    KEY idx_create_time (create_time),
    KEY idx_user (user_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '订单主表';


-- ============================================================
-- 订单明细 —— 一单多商品，下单后基本不变
-- ============================================================
CREATE TABLE IF NOT EXISTS order_detail (
    id                  BIGINT          NOT NULL AUTO_INCREMENT COMMENT '明细ID',
    order_id            BIGINT          NOT NULL                COMMENT '订单ID',
    sku_id              BIGINT          NOT NULL                COMMENT '商品SKU',
    sku_num             INT             NOT NULL DEFAULT 1      COMMENT '数量',
    order_price         DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '成交单价',
    split_total_amount  DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '分摊后金额',
    create_time         DATETIME(3)     NOT NULL                COMMENT '创建时间',
    update_time         DATETIME(3)     NOT NULL                COMMENT '更新时间',

    PRIMARY KEY (id),
    KEY idx_order (order_id),
    KEY idx_update_time (update_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '订单明细表';


-- ============================================================
-- 支付表
-- ============================================================
CREATE TABLE IF NOT EXISTS payment_info (
    id                  BIGINT          NOT NULL AUTO_INCREMENT COMMENT '支付ID',
    order_id            BIGINT          NOT NULL                COMMENT '订单ID',
    user_id             BIGINT          NOT NULL                COMMENT '用户ID',
    payment_type        VARCHAR(16)     NOT NULL                COMMENT '支付方式：ALIPAY/WECHAT/CARD/CREDIT',
    payment_status      VARCHAR(16)     NOT NULL                COMMENT '支付状态：SUCCESS/FAILED',
    payment_amount      DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '支付金额',
    callback_time       DATETIME(3)     NULL                    COMMENT '支付回调时间',
    create_time         DATETIME(3)     NOT NULL                COMMENT '创建时间',
    update_time         DATETIME(3)     NOT NULL                COMMENT '更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY uk_order (order_id),
    KEY idx_update_time (update_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '支付表';


-- ============================================================
-- 退款表 —— 低频但金额敏感，实时链路要能立刻反映到 GMV 口径
-- ============================================================
CREATE TABLE IF NOT EXISTS refund_info (
    id                  BIGINT          NOT NULL AUTO_INCREMENT COMMENT '退款ID',
    order_id            BIGINT          NOT NULL                COMMENT '订单ID',
    user_id             BIGINT          NOT NULL                COMMENT '用户ID',
    refund_type         VARCHAR(16)     NOT NULL                COMMENT '退款类型：ONLY_REFUND/RETURN_REFUND',
    refund_status       VARCHAR(16)     NOT NULL                COMMENT '退款状态：APPLYING/SUCCESS/REJECTED',
    refund_amount       DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '退款金额',
    refund_reason       VARCHAR(64)     NULL                    COMMENT '退款原因',
    create_time         DATETIME(3)     NOT NULL                COMMENT '申请时间',
    update_time         DATETIME(3)     NOT NULL                COMMENT '更新时间',

    PRIMARY KEY (id),
    KEY idx_order (order_id),
    KEY idx_update_time (update_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '退款表';


-- ============================================================
-- 维表：商品 / 用户 / 省份
--
-- 维表也走 CDC 进 Flink，而不是让流作业反复查库 ——
-- 生产环境里维表变更同样需要实时感知（改价、下架都会影响口径）。
-- ============================================================
CREATE TABLE IF NOT EXISTS sku_info (
    id                  BIGINT          NOT NULL                COMMENT 'SKU ID',
    sku_name            VARCHAR(128)    NOT NULL                COMMENT '商品名',
    category3_id        INT             NOT NULL                COMMENT '三级品类ID',
    tm_id               INT             NOT NULL                COMMENT '品牌ID',
    price               DECIMAL(16, 2)  NOT NULL DEFAULT 0      COMMENT '标价',
    is_sale             TINYINT         NOT NULL DEFAULT 1      COMMENT '是否在售',
    create_time         DATETIME(3)     NOT NULL                COMMENT '创建时间',
    update_time         DATETIME(3)     NOT NULL                COMMENT '更新时间',
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '商品维表';

CREATE TABLE IF NOT EXISTS base_category3 (
    id                  INT             NOT NULL                COMMENT '三级品类ID',
    name                VARCHAR(64)     NOT NULL                COMMENT '三级品类名',
    category2_id        INT             NOT NULL                COMMENT '二级品类ID',
    category2_name      VARCHAR(64)     NOT NULL                COMMENT '二级品类名',
    category1_name      VARCHAR(64)     NOT NULL                COMMENT '一级品类名',
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '品类维表（已打平三级）';

CREATE TABLE IF NOT EXISTS user_info (
    id                  BIGINT          NOT NULL                COMMENT '用户ID',
    login_name          VARCHAR(64)     NOT NULL                COMMENT '登录名',
    gender              VARCHAR(8)      NULL                    COMMENT '性别',
    birthday            DATE            NULL                    COMMENT '生日',
    user_level          VARCHAR(8)      NULL                    COMMENT '会员等级',
    create_time         DATETIME(3)     NOT NULL                COMMENT '注册时间',
    update_time         DATETIME(3)     NOT NULL                COMMENT '更新时间',
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '用户维表';

CREATE TABLE IF NOT EXISTS base_province (
    id                  INT             NOT NULL                COMMENT '省份ID',
    name                VARCHAR(32)     NOT NULL                COMMENT '省份名',
    region_id           INT             NOT NULL                COMMENT '大区ID',
    region_name         VARCHAR(32)     NOT NULL                COMMENT '大区名',
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '省份维表';


-- ============================================================
-- 维表初始数据
-- ============================================================
INSERT IGNORE INTO base_province (id, name, region_id, region_name) VALUES
    (1,'北京',1,'华北'),(2,'天津',1,'华北'),(3,'河北',1,'华北'),(4,'山西',1,'华北'),
    (5,'上海',2,'华东'),(6,'江苏',2,'华东'),(7,'浙江',2,'华东'),(8,'安徽',2,'华东'),
    (9,'福建',2,'华东'),(10,'山东',2,'华东'),
    (11,'广东',3,'华南'),(12,'广西',3,'华南'),(13,'海南',3,'华南'),
    (14,'河南',4,'华中'),(15,'湖北',4,'华中'),(16,'湖南',4,'华中'),
    (17,'四川',5,'西南'),(18,'重庆',5,'西南'),(19,'云南',5,'西南'),(20,'贵州',5,'西南'),
    (21,'陕西',6,'西北'),(22,'甘肃',6,'西北'),(23,'新疆',6,'西北'),
    (24,'辽宁',7,'东北'),(25,'吉林',7,'东北'),(26,'黑龙江',7,'东北');

INSERT IGNORE INTO base_category3 (id, name, category2_id, category2_name, category1_name) VALUES
    (1,'手机','10','手机通讯','手机数码'),
    (2,'笔记本','11','电脑办公','手机数码'),
    (3,'平板电脑','11','电脑办公','手机数码'),
    (4,'耳机耳麦','12','数码配件','手机数码'),
    (5,'T恤','20','男装','服饰内衣'),
    (6,'连衣裙','21','女装','服饰内衣'),
    (7,'运动鞋','22','鞋靴','服饰内衣'),
    (8,'面部护肤','30','美妆护肤','个护化妆'),
    (9,'洗发护发','31','清洁洗护','个护化妆'),
    (10,'零食礼包','40','休闲食品','食品生鲜'),
    (11,'牛奶乳品','41','乳品饮料','食品生鲜'),
    (12,'空调','50','大家电','家用电器'),
    (13,'电饭煲','51','厨房小电','家用电器'),
    (14,'图书','60','文学','图书文娱'),
    (15,'母婴用品','70','奶粉辅食','母婴玩具');
