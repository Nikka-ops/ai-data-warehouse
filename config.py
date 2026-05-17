# -*- coding: utf-8 -*-
"""
集中配置管理 - 所有配置从环境变量读取
使用方式：from config import cfg
"""
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

_BASE = os.path.dirname(os.path.abspath(__file__))


@dataclass(frozen=True)
class _Config:
    # ── ClickHouse ────────────────────────────────────────────
    ch_host: str     = field(default_factory=lambda: os.getenv('CLICKHOUSE_HOST', 'localhost'))
    ch_port: int     = field(default_factory=lambda: int(os.getenv('CLICKHOUSE_PORT', '8123')))
    ch_user: str     = field(default_factory=lambda: os.getenv('CLICKHOUSE_USER', 'admin'))
    ch_password: str = field(default_factory=lambda: os.getenv('CLICKHOUSE_PASSWORD', 'admin123'))

    # ── LLM (DeepSeek / OpenAI-compatible) ───────────────────
    api_key: str      = field(default_factory=lambda: os.getenv('DEEPSEEK_API_KEY', ''))
    api_base_url: str = field(default_factory=lambda: os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com'))
    llm_model: str    = field(default_factory=lambda: os.getenv('LLM_MODEL', 'deepseek-chat'))

    # ── AI 超参 ───────────────────────────────────────────────
    nl2sql_temperature: float  = field(default_factory=lambda: float(os.getenv('NL2SQL_TEMPERATURE', '0.1')))
    insight_temperature: float = field(default_factory=lambda: float(os.getenv('INSIGHT_TEMPERATURE', '0.7')))
    rag_temperature: float     = field(default_factory=lambda: float(os.getenv('RAG_TEMPERATURE', '0.3')))
    agent_temperature: float   = field(default_factory=lambda: float(os.getenv('AGENT_TEMPERATURE', '0.3')))
    rag_top_k: int             = field(default_factory=lambda: int(os.getenv('RAG_TOP_K', '3')))
    chunk_size: int            = field(default_factory=lambda: int(os.getenv('RAG_CHUNK_SIZE', '400')))
    chunk_overlap: int         = field(default_factory=lambda: int(os.getenv('RAG_CHUNK_OVERLAP', '80')))
    schema_cache_ttl: int      = field(default_factory=lambda: int(os.getenv('SCHEMA_CACHE_TTL', '3600')))

    # ── Kafka ─────────────────────────────────────────────────
    kafka_bootstrap: str    = field(default_factory=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
    orders_topic: str       = field(default_factory=lambda: os.getenv('KAFKA_ORDERS_TOPIC', 'orders_stream'))
    payments_topic: str     = field(default_factory=lambda: os.getenv('KAFKA_PAYMENTS_TOPIC', 'payments_stream'))
    flink_stats_topic: str  = field(default_factory=lambda: os.getenv('KAFKA_FLINK_STATS_TOPIC', 'flink.minute_stats'))
    flink_dwd_topic: str    = field(default_factory=lambda: os.getenv('KAFKA_FLINK_DWD_TOPIC', 'flink.realtime_dwd'))
    flink_alerts_topic: str = field(default_factory=lambda: os.getenv('KAFKA_FLINK_ALERTS_TOPIC', 'flink.alerts'))

    # ── ETL ───────────────────────────────────────────────────
    etl_batch_size: int = field(default_factory=lambda: int(os.getenv('ETL_BATCH_SIZE', '10000')))

    # ── 路径 ──────────────────────────────────────────────────
    base_dir: str       = _BASE
    knowledge_dir: str  = os.path.join(_BASE, 'knowledge_base')
    chroma_dir: str     = os.path.join(_BASE, 'chroma_db')
    reports_dir: str    = os.path.join(_BASE, 'reports')
    data_dir: str       = os.path.join(_BASE, 'data', 'raw')

    # ── Auto ETL 调度 ─────────────────────────────────────────
    etl_timezone: str   = field(default_factory=lambda: os.getenv('ETL_TIMEZONE', 'America/Sao_Paulo'))
    etl_ods_cron: str   = field(default_factory=lambda: os.getenv('ETL_ODS_CRON', '0 1 * * *'))   # 每天01:00
    etl_dwd_cron: str   = field(default_factory=lambda: os.getenv('ETL_DWD_CRON', '0 2 * * *'))   # 每天02:00
    etl_ads_cron: str   = field(default_factory=lambda: os.getenv('ETL_ADS_CRON', '0 3 * * *'))   # 每天03:00


cfg = _Config()
