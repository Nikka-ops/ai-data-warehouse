# ADR-001: 选择 Flink 而非 Spark Streaming

## 状态
已接受

## 背景
需要选择流处理引擎，支持实时处理和历史回放双模式。

## 决策
选择 Apache Flink 1.18。

## 理由
- **事件时间语义**：Flink 原生支持事件时间和水印，Spark 需要额外配置
- **有状态计算**：Flink RocksDB 状态后端支持超大状态，适合用户特征计算
- **低延迟**：Flink 毫秒级延迟，Spark 微批至少 100ms
- **回放支持**：Flink 可从 Kafka offset=earliest 重放，无需修改作业代码

## 代价
- Java 生态，Python API 是二等公民
- 运维复杂度高于 Spark Structured Streaming
