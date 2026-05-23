from fastapi import APIRouter
from src.api.rest.schemas import LineageRequest

router = APIRouter(prefix="/lineage", tags=["血缘"])

@router.post("/query")
async def query_lineage(req: LineageRequest):
    """查询表血缘关系"""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../..'))
    from ai_layer.lineage import get_lineage, get_upstream, get_downstream
    if req.direction == "upstream":
        return {"result": get_upstream(req.table_name)}
    elif req.direction == "downstream":
        return {"result": get_downstream(req.table_name)}
    return {"result": get_lineage(req.table_name)}

@router.get("/impact/{table_name}")
async def impact_analysis(table_name: str):
    """分析表变更的下游影响"""
    from src.lineage.graph.lineage_graph import LineageGraph
    from src.lineage.analyzer.impact_analyzer import ImpactAnalyzer
    graph = LineageGraph.from_sql_files("")
    return ImpactAnalyzer(graph).analyze(table_name)
