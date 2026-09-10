-- ============================================================
-- 01 建库
--
-- 分层只保留实时链路需要的四层：
--
--   dwd     明细层 —— 订单累积快照（Flink 从 CDC 直写）
--   dws     汇总层 —— 窗口聚合结果 + BITMAP 日活
--   ads     应用层 —— 看板 / NL2SQL 直接查的视图与结果表
--   stream  运维层 —— 迟到数据、作业指标、质检告警、风控结果
--
-- 没有 ODS 库是刻意的：实时链路的 ODS 落在 Kafka（dwd_trade_* topic），
-- 不落 OLAP。ODS 是给下游流作业消费的中间态，不是给人查的，
-- 每多落一次库就多一次写入与读取，延迟逐层累加。
--
-- 维表也不在 Doris：Flink 直接用 mysql-cdc 读业务库的维表做 lookup join，
-- 改价、下架能被实时感知，不需要在 Doris 里再存一份会过期的副本。
-- ============================================================

CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS dws;
CREATE DATABASE IF NOT EXISTS ads;
CREATE DATABASE IF NOT EXISTS stream;
