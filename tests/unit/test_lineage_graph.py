# -*- coding: utf-8 -*-
"""数据血缘图单元测试"""
import pytest
import sys
sys.path.insert(0, '/home/user/ai-data-warehouse')

class TestLineageGraph:
    def setup_method(self):
        """构建测试用血缘图"""
        try:
            from src.lineage.graph.lineage_graph import LineageGraph
            from src.lineage.graph.node import LineageNode, NodeType
            from src.lineage.graph.edge import LineageEdge, EdgeType
        except ImportError:
            pytest.skip("src.lineage 模块未安装")

        self.graph = LineageGraph()
        nodes = [
            LineageNode("kafka.orders", "orders_stream", NodeType.KAFKA_TOPIC),
            LineageNode("ods.orders",   "ods.orders_stream", NodeType.TABLE),
            LineageNode("dwd.orders",   "dwd.realtime_order_detail", NodeType.TABLE),
            LineageNode("dws.stats",    "dws.realtime_minute_stats", NodeType.TABLE),
            LineageNode("ads.hourly",   "ads.realtime_hourly", NodeType.VIEW),
        ]
        for n in nodes:
            self.graph.add_node(n)

        edges = [
            LineageEdge("kafka.orders", "ods.orders",  EdgeType.WRITES_TO),
            LineageEdge("ods.orders",   "dwd.orders",  EdgeType.DERIVED_FROM),
            LineageEdge("dwd.orders",   "dws.stats",   EdgeType.DERIVED_FROM),
            LineageEdge("dws.stats",    "ads.hourly",  EdgeType.DERIVED_FROM),
        ]
        for e in edges:
            self.graph.add_edge(e)

    def test_downstream_from_kafka(self):
        downstream = self.graph.get_downstream("kafka.orders")
        assert "dws.stats" in downstream
        assert "ads.hourly" in downstream

    def test_upstream_of_ads(self):
        upstream = self.graph.get_upstream("ads.hourly")
        assert "kafka.orders" in upstream
        assert "ods.orders" in upstream

    def test_impact_score_high_for_kafka(self):
        score = self.graph.get_impact_score("kafka.orders")
        assert score > 0, "Kafka Topic 影响分数应大于 0"

    def test_leaf_node_has_no_downstream(self):
        downstream = self.graph.get_downstream("ads.hourly")
        assert len(downstream) == 0
