from pydantic import BaseModel, Field
from typing import Any
from datetime import datetime

class QueryRequest(BaseModel):
    question: str = Field(..., description="自然语言查询")
    max_rows: int = Field(100, le=1000)
    use_cache: bool = True

class QueryResponse(BaseModel):
    sql: str
    data: list[dict]
    row_count: int
    elapsed_ms: float
    insight: str = ""
    confidence: float = 1.0

class AlertRequest(BaseModel):
    source: str
    severity: str = "P3"
    title: str
    detail: str = ""
    metric_name: str = ""
    current_value: float = 0.0

class AlertResponse(BaseModel):
    alert_id: str
    diagnosis: str
    actions: list[str]
    escalated: bool

class LineageRequest(BaseModel):
    table_name: str
    direction: str = "both"  # upstream|downstream|both
    depth: int = Field(3, le=10)

class FeatureRequest(BaseModel):
    entity_type: str   # user|seller|category
    entity_id: str
    feature_names: list[str] = []

class HealthResponse(BaseModel):
    status: str
    clickhouse: bool
    redis: bool
    kafka: bool
    timestamp: datetime
