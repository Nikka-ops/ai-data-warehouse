from fastapi import APIRouter, HTTPException
from src.api.rest.schemas import QueryRequest, QueryResponse

router = APIRouter(prefix="/query", tags=["查询"])

@router.post("/nl2sql", response_model=QueryResponse)
async def natural_language_query(req: QueryRequest):
    """自然语言转 SQL 查询"""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))
    from ai_layer.nl2sql import nl2sql  # 函数名为 nl2sql，非 nl2sql_query
    result = nl2sql(req.question)
    # nl2sql 返回 data 为 pd.DataFrame，需转换为 list[dict]
    data = result.get("data", [])
    if hasattr(data, "to_dict"):
        data = data.to_dict(orient="records")
    return QueryResponse(
        sql=result.get("sql", ""),
        data=data,
        row_count=result.get("row_count", 0),
        elapsed_ms=result.get("elapsed_ms", 0),
        insight=result.get("insight", ""),
        confidence=result.get("insight_confidence", 1.0),
    )

@router.post("/sql")
async def raw_sql_query(sql: str):
    """直接执行 SQL（仅 SELECT）"""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))
    from ai_layer.nl2sql import validate_sql
    try:
        validate_sql(sql)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    from src.api.rest.dependencies import get_ch_client
    ch = get_ch_client()
    result = ch.query(sql)
    return {"data": result.result_rows, "columns": result.column_names}
