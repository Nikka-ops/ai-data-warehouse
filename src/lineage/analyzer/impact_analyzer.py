# -*- coding: utf-8 -*-
"""表级影响分析器"""
from src.lineage.graph.lineage_graph import LineageGraph


class ImpactAnalyzer:
    def __init__(self, graph: LineageGraph):
        self.graph = graph

    def analyze(self, affected_table: str) -> dict:
        """分析一张表异常对下游的影响"""
        downstream = self.graph.get_downstream(affected_table)
        upstream = self.graph.get_upstream(affected_table)
        impact_score = self.graph.get_impact_score(affected_table)
        return {
            "affected_table": affected_table,
            "upstream_count": len(upstream),
            "downstream_count": len(downstream),
            "downstream_tables": downstream,
            "impact_score": impact_score,
            "severity": "P1" if impact_score > 0.7 else "P2" if impact_score > 0.3 else "P3",
        }
