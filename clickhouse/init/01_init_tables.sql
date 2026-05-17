-- ============================================================
-- 数据库初始化（实时架构）
-- 所有表结构由 02_kafka_stream.sql 和 03_flink_realtime.sql 创建
-- 本文件仅确保数据库存在（Docker 按文件名顺序执行）
-- ============================================================

CREATE DATABASE IF NOT EXISTS ods;     -- 实时原始层（Kafka 消费落地）
CREATE DATABASE IF NOT EXISTS dwd;     -- 实时明细层（Flink JOIN 宽表）
CREATE DATABASE IF NOT EXISTS dws;     -- 实时汇总层（Flink 分钟窗口聚合）
CREATE DATABASE IF NOT EXISTS ads;     -- 应用层（实时视图，供 NL2SQL 查询）
CREATE DATABASE IF NOT EXISTS stream;  -- 流处理专用库（Kafka Engine + 告警）
