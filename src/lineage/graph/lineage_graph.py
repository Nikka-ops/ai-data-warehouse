# -*- coding: utf-8 -*-
"""基于 NetworkX 的数据血缘图"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

try:
    import networkx as nx
    HAS_NX = True
except ImportError:
    HAS_NX = False

from src.lineage.graph.node import LineageNode, NodeType
from src.lineage.graph.edge import LineageEdge, EdgeType


class LineageGraph:
    def __init__(self):
        self._g = nx.DiGraph() if HAS_NX else None
        self._nodes: dict[str, LineageNode] = {}
        self._edges: list[LineageEdge] = []

    def add_node(self, node: LineageNode):
        self._nodes[node.id] = node
        if self._g:
            self._g.add_node(node.id, **vars(node))

    def add_edge(self, edge: LineageEdge):
        self._edges.append(edge)
        if self._g:
            self._g.add_edge(edge.source, edge.target, edge_type=edge.edge_type.value)

    def get_upstream(self, node_id: str, depth: int = 3) -> list[str]:
        if not self._g or node_id not in self._g:
            return []
        return list(nx.ancestors(self._g, node_id))

    def get_downstream(self, node_id: str, depth: int = 3) -> list[str]:
        if not self._g or node_id not in self._g:
            return []
        return list(nx.descendants(self._g, node_id))

    def get_impact_score(self, node_id: str) -> float:
        """节点的下游影响评分（基于下游节点数量）"""
        downstream = self.get_downstream(node_id)
        return min(len(downstream) / 10.0, 1.0)

    @classmethod
    def from_sql_files(cls, sql_dir: str) -> "LineageGraph":
        """从 SQL 初始化文件解析血缘"""
        from ai_layer.lineage import get_lineage
        g = cls()
        # 调用现有解析器
        try:
            lineage_data = get_lineage()  # 获取全局血缘
            # 将旧格式 Node/Edge 转换为新 LineageNode/LineageEdge
            for n in lineage_data.get('nodes', []):
                ntype = NodeType.VIEW if n.node_type == 'view' else NodeType.TABLE
                g.add_node(LineageNode(
                    id=n.name,
                    name=n.name,
                    node_type=ntype,
                    database=n.db,
                ))
            for e in lineage_data.get('edges', []):
                g.add_edge(LineageEdge(
                    source=e.source,
                    target=e.target,
                    edge_type=EdgeType.DERIVED_FROM,
                    transform=e.edge_type,
                ))
        except Exception:
            pass
        return g
