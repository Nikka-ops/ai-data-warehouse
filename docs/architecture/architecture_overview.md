# AI Data Warehouse 架构概览

## 核心设计理念

本项目采用 **Kappa 架构**（单一流处理引擎），区别于传统 Lambda 架构的双路径设计：
- 实时路径：Kafka → Flink（事件时间处理）→ ClickHouse
- 历史回放：Kafka（RETENTION=-1）→ Flink（--replay 模式）→ ClickHouse（幂等写入）

## 分层架构

```
┌──────────────────────────────────────┐
│            服务层                     │  FastAPI / Streamlit / Grafana
├──────────────────────────────────────┤
│          Agent 编排层                 │  LangGraph Supervisor
│  SQL Agent │ RAG Agent │ Alert Agent  │
├──────────────────────────────────────┤
│          Feature Store               │  Feast + Redis + ClickHouse
├──────────────────────────────────────┤
│  ClickHouse  │  Redis  │  Iceberg     │  三级存储
├──────────────────────────────────────┤
│          流处理层（Flink）             │  EXACTLY_ONCE, RocksDB
├──────────────────────────────────────┤
│          接入层（Kafka）              │  RETENTION=-1, 永久日志
└──────────────────────────────────────┘
```

## 关键设计决策

详见 `decision_records/` 目录下的 ADR 文档。
