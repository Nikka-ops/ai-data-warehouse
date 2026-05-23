# -*- coding: utf-8 -*-
"""血缘查询工具（包装现有 ai_layer/lineage.py）"""
from langchain_core.tools import tool


@tool
def get_table_lineage(table_name: str) -> str:
    """查询指定表的完整数据血缘（上游 + 下游）"""
    try:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
        from ai_layer.lineage import get_lineage
        result = get_lineage()
        table_lower = table_name.lower()
        upstream = [e.source for e in result['edges'] if e.target == table_lower]
        downstream = [e.target for e in result['edges'] if e.source == table_lower]
        return (
            f"表 {table_name} 的血缘关系：\n"
            f"上游（{len(upstream)} 个）：{', '.join(upstream) or '无'}\n"
            f"下游（{len(downstream)} 个）：{', '.join(downstream) or '无'}"
        )
    except Exception as e:
        return f"血缘查询失败: {e}"


@tool
def get_impact_analysis(table_name: str) -> str:
    """分析指定表发生变化时对下游的影响范围"""
    try:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
        from ai_layer.lineage import get_downstream, get_lineage
        downstream = get_downstream(table_name)
        # 递归查找二级下游
        second_level = []
        for t in downstream:
            second_level.extend(get_downstream(t))
        second_level = list(set(second_level) - set(downstream))
        return (
            f"表 {table_name} 的影响分析：\n"
            f"直接下游（{len(downstream)} 个）：{', '.join(downstream) or '无'}\n"
            f"间接下游（{len(second_level)} 个）：{', '.join(second_level) or '无'}\n"
            f"总影响范围：{len(downstream) + len(second_level)} 个表"
        )
    except Exception as e:
        return f"影响分析失败: {e}"
