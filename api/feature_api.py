#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
特征服务 API — Feature Serving API
提供低延迟在线特征读取 + 批量特征导出接口

端点：
  GET  /features/online/{group}/{entity_id}          在线特征（Redis，p99 < 20ms）
  POST /features/online/batch                         批量在线特征（最多 1000 个实体）
  GET  /features/groups                              列举所有特征组
  GET  /features/definitions/{group}                 列举特征组内所有特征
  POST /features/dataset/build                       触发训练数据集构建
  GET  /features/dataset/list                        列举已构建数据集
  POST /features/suggest                             AI 推荐新特征（触发 Auto Feature Engineering）
  GET  /features/drift/{group}                       特征漂移状态
  GET  /health                                       健康检查
"""
import os, sys, json, time
from typing import Optional
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger

log = get_logger('feature_api')

try:
    from fastapi import FastAPI, HTTPException, BackgroundTasks
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
except ImportError:
    raise ImportError("pip install fastapi uvicorn")

app = FastAPI(
    title="AI 数仓 · 特征服务 API",
    description="Kappa 架构特征存储的在线服务层，支持低延迟特征读取和批量导出",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ── 请求/响应模型 ────────────────────────────────────────────

class BatchFeatureRequest(BaseModel):
    entity_ids: list[str] = Field(..., max_length=1000)
    group_name: str
    feature_names: Optional[list[str]] = None

class DatasetBuildRequest(BaseModel):
    dataset_name: str
    label_sql: str
    feature_groups: list[str]
    description: str = ''

class FeatureSuggestRequest(BaseModel):
    business_goal: str = '预测用户订单取消风险和GMV潜力'

class FeatureResponse(BaseModel):
    entity_id: str
    group_name: str
    features: dict
    freshness: dict = {}
    latency_ms: float = 0

# ── 依赖懒加载 ────────────────────────────────────────────────

_online_store = None
_registry = None
_drift_monitor = None

def _get_online():
    global _online_store
    if _online_store is None:
        from feature_store.online_store import OnlineFeatureStore
        _online_store = OnlineFeatureStore()
    return _online_store

def _get_registry():
    global _registry
    if _registry is None:
        from feature_store.registry import FeatureRegistry
        _registry = FeatureRegistry()
        _registry.load_all()
    return _registry


# ── 端点实现 ──────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}


@app.get("/features/groups")
def list_groups():
    """列举所有注册的特征组"""
    try:
        groups = _get_registry().list_feature_groups()
        return {"groups": groups, "total": len(groups)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/features/definitions/{group_name}")
def list_features(group_name: str):
    """列举特征组内所有特征定义"""
    try:
        features = _get_registry().list_features(group_name)
        return {"group_name": group_name, "features": features, "total": len(features)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/features/online/{group_name}/{entity_id}", response_model=FeatureResponse)
def get_online_features(group_name: str, entity_id: str,
                        features: Optional[str] = None):
    """
    在线特征读取（Redis → ClickHouse fallback）
    - features: 逗号分隔的特征名列表（不传则返回该组所有特征）
    - 目标 p99 < 20ms
    """
    t0 = time.monotonic()
    feature_names = features.split(',') if features else None
    try:
        result = _get_online().get_features(entity_id, group_name, feature_names)
        freshness = _get_online().get_freshness(entity_id, group_name)
        latency = (time.monotonic() - t0) * 1000
        return FeatureResponse(
            entity_id=entity_id,
            group_name=group_name,
            features=result,
            freshness=freshness,
            latency_ms=round(latency, 2),
        )
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/features/online/batch")
def get_batch_features(req: BatchFeatureRequest):
    """批量在线特征读取（最多 1000 个实体，使用 Redis pipeline）"""
    t0 = time.monotonic()
    if len(req.entity_ids) > 1000:
        raise HTTPException(400, "每次最多请求 1000 个实体")
    try:
        results = _get_online().get_multi_entity_features(
            req.entity_ids, req.group_name, req.feature_names
        )
        latency = (time.monotonic() - t0) * 1000
        return {
            "group_name": req.group_name,
            "results": results,
            "entity_count": len(results),
            "latency_ms": round(latency, 2),
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/features/dataset/build")
async def build_dataset(req: DatasetBuildRequest, background: BackgroundTasks):
    """触发训练数据集构建（异步后台执行）"""
    import uuid
    task_id = str(uuid.uuid4())[:8]

    def _run():
        from feature_store.dataset_builder import DatasetBuilder
        import tempfile, os
        out_path = os.path.join(tempfile.gettempdir(), f'{req.dataset_name}_{task_id}.parquet')
        builder = DatasetBuilder()
        result = builder.build(
            dataset_name=req.dataset_name,
            label_sql=req.label_sql,
            feature_groups=req.feature_groups,
            output_path=out_path,
            description=req.description,
        )
        log.info('数据集构建完成：%s', result)

    background.add_task(_run)
    return {"task_id": task_id, "status": "accepted",
            "message": f"数据集 {req.dataset_name} 构建中，用 /features/dataset/list 查询结果"}


@app.get("/features/dataset/list")
def list_datasets():
    """列举已构建的训练数据集"""
    try:
        from feature_store.dataset_builder import DatasetBuilder
        datasets = DatasetBuilder().list_datasets()
        return {"datasets": datasets, "total": len(datasets)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/features/suggest")
async def suggest_features(req: FeatureSuggestRequest, background: BackgroundTasks):
    """AI 推荐新特征（LLM 分析现有数据，自动生成特征 YAML）"""
    def _run():
        from feature_store.auto_feature import AutoFeatureEngineer
        result = AutoFeatureEngineer().run(req.business_goal)
        log.info('特征建议完成：%s', result)

    background.add_task(_run)
    return {"status": "accepted",
            "message": f"正在分析业务目标「{req.business_goal}」，结果将保存到 features/generated/"}


@app.get("/features/drift/{group_name}")
def get_drift_status(group_name: str):
    """查询特征漂移状态"""
    try:
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=cfg.ch_host, port=cfg.ch_port,
            username=cfg.ch_user, password=cfg.ch_password,
        )
        rows = ch.query(f"""
            SELECT feature_name, check_time, mean_value, std_value,
                   psi_score, drift_detected
            FROM feature_store.drift_stats
            WHERE group_name = '{group_name}'
            ORDER BY check_time DESC
            LIMIT 20
        """).result_rows
        return {
            "group_name": group_name,
            "drift_stats": [
                {"feature": r[0], "check_time": str(r[1]),
                 "mean": round(float(r[2]), 4), "std": round(float(r[3]), 4),
                 "psi": round(float(r[4]), 4), "drift": bool(r[5])}
                for r in rows
            ]
        }
    except Exception as e:
        raise HTTPException(500, str(e))


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000, log_level='info')
