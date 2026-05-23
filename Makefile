.PHONY: build start stop restart logs test lint clean deploy-k8s

# 环境变量
COMPOSE=docker compose
ENV_FILE=.env

build:
	$(COMPOSE) build --no-cache

start:
	$(COMPOSE) --env-file $(ENV_FILE) up -d

stop:
	$(COMPOSE) down

restart: stop start

logs:
	$(COMPOSE) logs -f --tail=100

test:
	pytest tests/unit -v
	pytest tests/integration -v --timeout=60

lint:
	ruff check src/ tests/
	mypy src/ --ignore-missing-imports

clean:
	$(COMPOSE) down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete

deploy-k8s:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/clickhouse/
	kubectl apply -f k8s/kafka/
	kubectl apply -f k8s/flink/
	kubectl apply -f k8s/agent/
	kubectl apply -f k8s/monitoring/

k8s-status:
	kubectl get pods -n ai-warehouse

flink-job:
	bash scripts/run_flink_job.sh

backfill:
	bash scripts/trigger_backfill.sh

benchmark:
	python src/scripts/benchmark.py

mock-data:
	python src/scripts/generate_mock_data.py
