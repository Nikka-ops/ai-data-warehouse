# -*- coding: utf-8 -*-
"""将内部血缘数据转换为 OpenLineage 标准格式"""
from datetime import datetime
from src.lineage.graph.lineage_graph import LineageGraph


class OpenLineageAdapter:
    """将 LineageGraph 导出为 OpenLineage 事件格式"""

    def __init__(self, graph: LineageGraph, namespace: str = "ai-warehouse"):
        self.graph = graph
        self.namespace = namespace

    def to_run_event(self, job_name: str, input_tables: list[str],
                     output_tables: list[str]) -> dict:
        """生成 OpenLineage RunEvent"""
        return {
            "eventType": "COMPLETE",
            "eventTime": datetime.utcnow().isoformat() + "Z",
            "run": {"runId": self._gen_run_id(job_name)},
            "job": {"namespace": self.namespace, "name": job_name},
            "inputs":  [{"namespace": self.namespace, "name": t} for t in input_tables],
            "outputs": [{"namespace": self.namespace, "name": t} for t in output_tables],
        }

    def _gen_run_id(self, job_name: str) -> str:
        import hashlib
        import uuid
        return str(uuid.UUID(hashlib.md5(job_name.encode()).hexdigest()))
