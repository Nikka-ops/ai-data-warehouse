from pydantic import BaseModel, Field

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
