# ADR-002: 选择 ClickHouse 而非 Apache Doris

## 状态
已接受

## 背景
需要 OLAP 数据库支持实时写入和高性能分析查询。

## 决策
选择 ClickHouse 24.3。

## 理由
- **社区成熟度**：ClickHouse 生产案例更丰富，文档完整
- **ReplacingMergeTree**：天然支持幂等写入，适合 Kappa 架构重放
- **Kafka Engine**：原生支持 Kafka 直接消费，无需额外 ETL
- **查询性能**：列式存储 + 向量化执行，亿级数据秒级响应

## 代价
- 单节点写入吞吐低于 Doris
- 分布式事务支持较弱
