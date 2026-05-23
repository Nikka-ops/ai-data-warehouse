# -*- coding: utf-8 -*-
"""
基于 Pydantic BaseSettings 的统一配置管理
优先读取 .env 文件，失败时降级到 os.environ.get
使用方式：from src.common.config import cfg
"""

import os

# 优先使用 pydantic_settings，否则降级为简单 dataclass
try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
    from pydantic import Field

    class Settings(BaseSettings):
        # ── ClickHouse 配置 ───────────────────────────────────────
        ch_host: str     = Field(default="localhost",  alias="CLICKHOUSE_HOST")
        ch_port: int     = Field(default=8123,         alias="CLICKHOUSE_PORT")
        ch_user: str     = Field(default="admin",      alias="CLICKHOUSE_USER")
        ch_password: str = Field(default="admin123",   alias="CLICKHOUSE_PASSWORD")

        # ── Redis 在线特征存储 ────────────────────────────────────
        redis_host: str = Field(default="localhost", alias="REDIS_HOST")
        redis_port: int = Field(default=6379,        alias="REDIS_PORT")

        # ── Kafka 消息队列 ────────────────────────────────────────
        kafka_bootstrap: str  = Field(default="localhost:9092",   alias="KAFKA_BOOTSTRAP_SERVERS")
        orders_topic: str     = Field(default="orders_stream",    alias="KAFKA_ORDERS_TOPIC")
        payments_topic: str   = Field(default="payments_stream",  alias="KAFKA_PAYMENTS_TOPIC")

        # ── LLM 接入 ──────────────────────────────────────────────
        api_key: str      = Field(default="",                          alias="DEEPSEEK_API_KEY")
        api_base_url: str = Field(default="https://api.deepseek.com",  alias="DEEPSEEK_BASE_URL")
        llm_model: str    = Field(default="deepseek-chat",             alias="LLM_MODEL")

        # ── AI 超参数 ─────────────────────────────────────────────
        nl2sql_temperature: float  = Field(default=0.1,  alias="NL2SQL_TEMPERATURE")
        insight_temperature: float = Field(default=0.7,  alias="INSIGHT_TEMPERATURE")
        rag_temperature: float     = Field(default=0.3,  alias="RAG_TEMPERATURE")
        agent_temperature: float   = Field(default=0.3,  alias="AGENT_TEMPERATURE")
        rag_top_k: int             = Field(default=3,    alias="RAG_TOP_K")

        # ── MinIO / Iceberg 对象存储 ──────────────────────────────
        minio_endpoint: str   = Field(default="http://minio:9000",         alias="MINIO_ENDPOINT")
        minio_access_key: str = Field(default="minioadmin",                alias="MINIO_ACCESS_KEY")
        minio_secret_key: str = Field(default="minioadmin",                alias="MINIO_SECRET_KEY")
        iceberg_warehouse: str    = Field(default="s3://warehouse",            alias="ICEBERG_WAREHOUSE")
        iceberg_catalog_uri: str  = Field(default="http://iceberg-rest:8181",  alias="ICEBERG_CATALOG_URI")

        # ── Webhook 告警通知 ──────────────────────────────────────
        webhook_url: str = Field(default="", alias="WEBHOOK_URL")

        model_config = SettingsConfigDict(
            env_file=".env",
            extra="ignore",
            populate_by_name=True,  # 允许同时使用字段名和 alias
        )

    cfg = Settings()

except ImportError:
    # 降级方案：直接从环境变量读取
    from dataclasses import dataclass, field

    @dataclass(frozen=True)
    class Settings:  # type: ignore[no-redef]
        # ClickHouse
        ch_host: str     = field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
        ch_port: int     = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "8123")))
        ch_user: str     = field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "admin"))
        ch_password: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", "admin123"))
        # Redis
        redis_host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
        redis_port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
        # Kafka
        kafka_bootstrap: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        orders_topic: str    = field(default_factory=lambda: os.getenv("KAFKA_ORDERS_TOPIC", "orders_stream"))
        payments_topic: str  = field(default_factory=lambda: os.getenv("KAFKA_PAYMENTS_TOPIC", "payments_stream"))
        # LLM
        api_key: str      = field(default_factory=lambda: os.getenv("DEEPSEEK_API_KEY", ""))
        api_base_url: str = field(default_factory=lambda: os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"))
        llm_model: str    = field(default_factory=lambda: os.getenv("LLM_MODEL", "deepseek-chat"))
        # AI 超参
        nl2sql_temperature: float  = field(default_factory=lambda: float(os.getenv("NL2SQL_TEMPERATURE", "0.1")))
        insight_temperature: float = field(default_factory=lambda: float(os.getenv("INSIGHT_TEMPERATURE", "0.7")))
        rag_temperature: float     = field(default_factory=lambda: float(os.getenv("RAG_TEMPERATURE", "0.3")))
        agent_temperature: float   = field(default_factory=lambda: float(os.getenv("AGENT_TEMPERATURE", "0.3")))
        rag_top_k: int             = field(default_factory=lambda: int(os.getenv("RAG_TOP_K", "3")))
        # MinIO / Iceberg
        minio_endpoint: str   = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        minio_access_key: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        minio_secret_key: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        iceberg_warehouse: str   = field(default_factory=lambda: os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse"))
        iceberg_catalog_uri: str = field(default_factory=lambda: os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"))
        # Webhook
        webhook_url: str = field(default_factory=lambda: os.getenv("WEBHOOK_URL", ""))

    cfg = Settings()
