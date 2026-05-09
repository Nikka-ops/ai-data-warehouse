# 环境配置说明

## 1. 初始化 ClickHouse 流式表结构

用 DBeaver 连接 ClickHouse（localhost:8123，用户名 admin，密码 admin123），
执行 `clickhouse/init/01_init_tables.sql` 和 `clickhouse/init/02_kafka_stream.sql`。

## 2. 创建 Kafka Topic

```bash
docker exec ai_dw_kafka kafka-topics --create --topic orders_stream \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec ai_dw_kafka kafka-topics --create --topic payments_stream \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

## 3. 常见问题

**Q: Windows 下 Docker 挂载 E 盘失败？**
修改 docker-compose.yml，把本地目录挂载改为 Docker Volume，详见 docker-compose.yml 注释。

**Q: Kafka 引擎物化视图创建报类型错误？**
直接使用 `event_time` 不做转换，ClickHouse 会自动推断类型。

**Q: Agent 陷入死循环？**
确保使用 Tool Calling 模式（`create_tool_calling_agent`）而非 ReAct（`create_react_agent`）。
