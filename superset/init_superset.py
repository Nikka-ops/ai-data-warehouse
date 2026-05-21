#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Superset 自动初始化脚本
等待 Superset 就绪后，通过 REST API 自动配置 ClickHouse 数据源和核心数据集。
"""
import os, sys, time, json
import requests

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER   = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASS   = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin123")

CH_HOST     = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT     = os.getenv("CLICKHOUSE_PORT", "8123")
CH_USER     = os.getenv("CLICKHOUSE_USER", "admin")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin123")

# ── 核心数据集：表名 → 描述（供 Superset 展示）─────────────────
DATASETS = [
    # ── 实时流层（Kafka 落地 + Flink 实时处理）──────────────
    ("ods",    "orders_stream",            "实时订单流 ODS（Kafka 落地）"),
    ("ods",    "payments_stream",          "实时支付流 ODS（Kafka 落地）"),
    ("dwd",    "realtime_order_detail",    "订单+支付宽表 DWD（Flink JOIN）"),
    ("dws",    "realtime_minute_stats",    "分钟级实时聚合（Flink 1分钟窗口）"),
    ("dws",    "realtime_forecast",        "AI 预测数据（Holt 双指数平滑）"),
    ("ads",    "realtime_hourly",          "今日小时聚合视图"),
    ("ads",    "realtime_category_today",  "今日品类排行视图"),
    ("ads",    "realtime_state_today",     "今日州排行视图"),
    # ── Kappa 历史聚合层（Flink 回放 Kafka 后写入）──────────
    ("dws",    "kappa_hourly_agg",         "Kappa 小时级历史聚合（Flink 回放结果，幂等）"),
    ("dws",    "kappa_serving_unified",    "Kappa 统一服务视图（历史回放 + 实时互补）"),
    ("dws",    "kappa_daily_trend",        "Kappa 日级趋势视图（供历史分析图表）"),
    ("dws",    "kappa_category_stats",     "Kappa 品类维度视图（历史 + 实时合并）"),
    ("ads",    "kappa_current_totals",     "当前 GMV 汇总（Kappa 服务层快照）"),
    # ── Kappa 监控层（回放进度 + 消费 Lag）──────────────────
    ("stream", "kappa_replay_jobs",        "Kappa 历史回放任务记录"),
    ("stream", "kappa_consumer_lag",       "Kafka 消费者 Lag 监控（实时 + 回放）"),
    ("stream", "kappa_replay_status",      "Kappa 回放任务健康状态视图"),
    # ── AI 分析层 ────────────────────────────────────────────
    ("stream", "ai_quality_alerts",        "AI 质检告警（Flink 内嵌 AI 质量门控）"),
    ("stream", "alert_investigations",     "AI 告警自动排查记录"),
    ("stream", "proactive_insights",       "AI 主动洞察（每5分钟）"),
    ("stream", "etl_audit_log",            "ETL 审计日志"),
]


def _wait_for_superset(timeout: int = 120):
    print(f"等待 Superset 就绪（{SUPERSET_URL}）...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if r.status_code == 200:
                print("Superset 已就绪")
                return True
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError(f"Superset 在 {timeout}s 内未就绪")


def _login() -> str:
    r = requests.post(f"{SUPERSET_URL}/api/v1/security/login", json={
        "username": ADMIN_USER,
        "password": ADMIN_PASS,
        "provider": "db",
        "refresh": False,
    })
    r.raise_for_status()
    token = r.json()["access_token"]
    print(f"已登录 Superset（用户：{ADMIN_USER}）")
    return token


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _create_database(token: str) -> int:
    """创建 ClickHouse 数据库连接，返回 database_id"""
    # 检查是否已存在
    r = requests.get(f"{SUPERSET_URL}/api/v1/database/", headers=_headers(token))
    for db in r.json().get("result", []):
        if db["database_name"] == "AI数仓-ClickHouse":
            print(f"ClickHouse 数据源已存在（id={db['id']}），跳过创建")
            return db["id"]

    # clickhouse+connect://user:pass@host:port/default
    sqlalchemy_uri = (
        f"clickhousedb+connect://{CH_USER}:{CH_PASSWORD}"
        f"@{CH_HOST}:{CH_PORT}/default"
    )
    payload = {
        "database_name": "AI数仓-ClickHouse",
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_dml": False,
        "extra": json.dumps({
            "engine_params": {},
            "metadata_params": {},
            "schemas_allowed_for_file_upload": [],
        }),
    }
    r = requests.post(f"{SUPERSET_URL}/api/v1/database/",
                      headers=_headers(token), json=payload)
    if r.status_code not in (200, 201):
        print(f"  创建数据库失败：{r.status_code} {r.text[:200]}")
        return -1

    db_id = r.json()["id"]
    print(f"ClickHouse 数据源已创建（id={db_id}）")
    return db_id


def _create_dataset(token: str, db_id: int, schema: str, table: str, desc: str):
    """创建 Superset 数据集"""
    # 检查是否已存在
    r = requests.get(f"{SUPERSET_URL}/api/v1/dataset/",
                     params={"q": json.dumps({"filters": [
                         {"col": "table_name", "opr": "eq", "val": table}
                     ]})},
                     headers=_headers(token))
    existing = [d for d in r.json().get("result", [])
                if d["table_name"] == table and d.get("schema") == schema]
    if existing:
        print(f"  数据集 {schema}.{table} 已存在，跳过")
        return

    payload = {
        "database": db_id,
        "schema": schema,
        "table_name": table,
        "description": desc,
    }
    r = requests.post(f"{SUPERSET_URL}/api/v1/dataset/",
                      headers=_headers(token), json=payload)
    if r.status_code in (200, 201):
        print(f"  ✓ {schema}.{table}")
    else:
        print(f"  ✗ {schema}.{table}：{r.status_code} {r.text[:100]}")


def main():
    _wait_for_superset()
    time.sleep(3)  # 额外等待 Superset 完全初始化

    try:
        token = _login()
    except Exception as e:
        print(f"登录失败（可能 admin 用户尚未创建）：{e}")
        sys.exit(1)

    db_id = _create_database(token)
    if db_id < 0:
        print("数据库创建失败，退出")
        sys.exit(1)

    print("\n正在创建数据集...")
    for schema, table, desc in DATASETS:
        try:
            _create_dataset(token, db_id, schema, table, desc)
        except Exception as e:
            print(f"  ✗ {schema}.{table} 异常：{e}")

    print("\n初始化完成！访问 http://localhost:8088 使用 Superset。")
    print(f"账号：{ADMIN_USER} / {ADMIN_PASS}")
    print("数据源「AI数仓-ClickHouse」已配置，可在 SQL Lab 直接查询。")


if __name__ == "__main__":
    main()
