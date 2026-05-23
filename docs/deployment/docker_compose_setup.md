# Docker Compose 本地部署指南

## 前置要求
- Docker Engine 24+
- Docker Compose v2.20+
- 内存 >= 8GB（推荐 16GB）
- 磁盘空间 >= 20GB

## 快速启动

```bash
# 1. 克隆仓库
git clone https://github.com/nikka-ops/ai-data-warehouse.git
cd ai-data-warehouse

# 2. 配置环境变量
cp .env.example .env
# 编辑 .env，填入 DEEPSEEK_API_KEY

# 3. 一键启动
make start

# 4. 查看服务状态
make logs
```

## 服务访问地址

| 服务 | 地址 |
|------|------|
| AI Dashboard | http://localhost |
| Grafana | http://localhost/grafana |
| Flink UI | http://localhost/flink |
| Kafka UI | http://localhost/kafka |
| API Docs | http://localhost/api/docs |

## 精简模式（6 个服务）

```bash
docker compose -f docker-compose.lite.yml up -d
```
