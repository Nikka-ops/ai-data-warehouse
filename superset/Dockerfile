FROM apache/superset:3.1.0
USER root
# ClickHouse driver (clickhouse-connect is the modern driver)
RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    clickhouse-sqlalchemy==0.3.2
USER superset
