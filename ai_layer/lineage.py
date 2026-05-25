# -*- coding: utf-8 -*-
"""
数据血缘解析模块
解析 clickhouse/init/*.sql，提取表/视图间的依赖关系，供 Streamlit 可视化调用。
"""

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from utils.logger import get_logger

log = get_logger('lineage')

# ── SQL 初始化文件目录 ────────────────────────────────────────
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_INIT_DIR = os.path.join(_BASE_DIR, 'clickhouse', 'init')

# ── 数据结构 ─────────────────────────────────────────────────

@dataclass
class Node:
    name: str         # "ods.orders_stream"
    db: str           # "ods"
    table: str        # "orders_stream"
    node_type: str    # "table" / "view"
    source_file: str  # "02_kafka_stream.sql"


@dataclass
class Edge:
    source: str    # 上游节点 name
    target: str    # 下游节点 name
    edge_type: str  # "SELECT_FROM" / "INSERT_INTO" / "JOIN"


# ── 模块级缓存 ────────────────────────────────────────────────
_cache: Optional[dict] = None


# ── 正则模式 ─────────────────────────────────────────────────

# 匹配 CREATE TABLE/VIEW，支持各种 IF NOT EXISTS / OR REPLACE 写法
_RE_CREATE_TABLE = re.compile(
    r'\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
    r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

_RE_CREATE_VIEW = re.compile(
    r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?'
    r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

# TO target_table（物化视图写入目标）
_RE_MV_TO = re.compile(
    r'\bTO\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

# FROM / JOIN
_RE_FROM = re.compile(
    r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

_RE_JOIN = re.compile(
    r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

# INSERT INTO ... SELECT ... FROM
_RE_INSERT_INTO = re.compile(
    r'\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

# 需要排除的系统/特殊库
_SYSTEM_DBS = {'system', 'information_schema', 'INFORMATION_SCHEMA'}


def _strip_comments(sql: str) -> str:
    """移除行注释（-- ...）和块注释（/* ... */），保留换行以维持行号关系"""
    # 移除块注释
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    # 移除行注释（保留换行）
    sql = re.sub(r'--[^\n]*', '', sql)
    return sql


def _is_valid_table(name: str) -> bool:
    """过滤掉系统库和非 db.table 格式的名称"""
    if '.' not in name:
        return False
    db = name.split('.')[0].lower()
    return db not in {s.lower() for s in _SYSTEM_DBS}


def _parse_sql_file(filepath: str) -> tuple[list[Node], list[Edge]]:
    """
    解析单个 SQL 文件，返回 (nodes, edges)。
    解析策略：按分号切分为独立语句，逐语句分析上下游关系。
    """
    filename = os.path.basename(filepath)
    log.debug('解析文件: %s', filename)

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            raw = f.read()
    except Exception as e:
        log.warning('无法读取文件 %s: %s', filepath, e)
        return [], []

    clean = _strip_comments(raw)

    # 按分号切分语句（过滤空语句）
    statements = [s.strip() for s in clean.split(';') if s.strip()]

    nodes: list[Node] = []
    edges: list[Edge] = []

    for stmt in statements:
        stmt_nodes, stmt_edges = _parse_statement(stmt, filename)
        nodes.extend(stmt_nodes)
        edges.extend(stmt_edges)

    return nodes, edges


def _parse_statement(stmt: str, source_file: str) -> tuple[list[Node], list[Edge]]:
    """解析单条 SQL 语句，提取节点和边"""
    nodes: list[Node] = []
    edges: list[Edge] = []

    # ── 1. 判断语句类型，提取定义节点 ────────────────────────

    defined_node: Optional[str] = None   # 本语句定义的对象（如 CREATE TABLE xyz）
    node_type: Optional[str] = None
    is_mv = False  # 是否是物化视图

    # 物化视图（含 MATERIALIZED）
    mv_match = re.search(
        r'\bCREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?'
        r'([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
        stmt, re.IGNORECASE,
    )
    if mv_match:
        defined_node = mv_match.group(1).lower()
        node_type = 'view'
        is_mv = True
    else:
        # 普通 VIEW
        v_match = _RE_CREATE_VIEW.search(stmt)
        if v_match:
            defined_node = v_match.group(1).lower()
            node_type = 'view'
        else:
            # TABLE
            t_match = _RE_CREATE_TABLE.search(stmt)
            if t_match:
                defined_node = t_match.group(1).lower()
                node_type = 'table'

    if defined_node and _is_valid_table(defined_node):
        db, tbl = defined_node.split('.', 1)
        nodes.append(Node(
            name=defined_node,
            db=db,
            table=tbl,
            node_type=node_type,
            source_file=source_file,
        ))

    # ── 2. 提取物化视图 TO 目标（边：defined_node → to_target）
    if is_mv and defined_node:
        to_match = _RE_MV_TO.search(stmt)
        if to_match:
            to_target = to_match.group(1).lower()
            if _is_valid_table(to_target) and to_target != defined_node:
                # 物化视图往目标表写入：视图 → 目标表（INSERT_INTO 语义）
                edges.append(Edge(
                    source=defined_node,
                    target=to_target,
                    edge_type='INSERT_INTO',
                ))

    # ── 3. 提取 INSERT INTO 边（source 是 FROM/JOIN 的上游，target 是 INSERT 目标）
    insert_matches = _RE_INSERT_INTO.findall(stmt)
    for tgt in insert_matches:
        tgt = tgt.lower()
        if not _is_valid_table(tgt):
            continue
        # 找 SELECT ... FROM 里的上游
        from_sources = [t.lower() for t in _RE_FROM.findall(stmt) if _is_valid_table(t)]
        for src in from_sources:
            if src != tgt:
                edges.append(Edge(source=src, target=tgt, edge_type='INSERT_INTO'))

    # ── 4. 提取 FROM / JOIN 依赖（当 defined_node 存在时，来源依赖于定义节点）
    if defined_node and _is_valid_table(defined_node):
        from_tables = [t.lower() for t in _RE_FROM.findall(stmt) if _is_valid_table(t)]
        join_tables = [t.lower() for t in _RE_JOIN.findall(stmt) if _is_valid_table(t)]

        # 物化视图写入 TO 目标时：FROM 上游 → MV → TO 目标
        # 常规视图/建表 SELECT：FROM 上游 → defined_node
        for src in from_tables:
            if src != defined_node:
                edges.append(Edge(source=src, target=defined_node, edge_type='SELECT_FROM'))

        for src in join_tables:
            if src != defined_node:
                edges.append(Edge(source=src, target=defined_node, edge_type='JOIN'))

    return nodes, edges


def _parse_all() -> dict:
    """扫描所有 SQL 文件，合并节点和边，返回血缘图"""
    sql_dir = Path(SQL_INIT_DIR)
    if not sql_dir.exists():
        log.warning('SQL 初始化目录不存在: %s', SQL_INIT_DIR)
        return {'nodes': [], 'edges': []}

    sql_files = sorted(sql_dir.glob('*.sql'))
    log.info('发现 %d 个 SQL 文件，开始解析...', len(sql_files))

    all_nodes: dict[str, Node] = {}   # name → Node（去重）
    all_edges: list[Edge] = []
    seen_edges: set[tuple] = set()

    for sql_file in sql_files:
        nodes, edges = _parse_sql_file(str(sql_file))
        for node in nodes:
            # 同名节点以最后出现的文件为准（或首次出现，均可；这里取首次）
            if node.name not in all_nodes:
                all_nodes[node.name] = node

        for edge in edges:
            key = (edge.source, edge.target, edge.edge_type)
            if key not in seen_edges:
                seen_edges.add(key)
                all_edges.append(edge)

    # 过滤边中引用了未知节点的情况：保留，允许外部表出现在边中
    # （可视化层根据需要决定是否展示孤立节点）

    log.info(
        '血缘解析完成：%d 个节点，%d 条边',
        len(all_nodes), len(all_edges),
    )
    return {
        'nodes': list(all_nodes.values()),
        'edges': all_edges,
    }


# ── 对外接口 ─────────────────────────────────────────────────

def get_lineage() -> dict:
    """
    返回 {"nodes": [Node], "edges": [Edge]}
    结果缓存在模块级变量，避免重复解析文件。
    """
    global _cache
    if _cache is None:
        _cache = _parse_all()
    return _cache


def invalidate_cache():
    """清除缓存，下次调用 get_lineage() 时重新解析"""
    global _cache
    _cache = None
    log.info('血缘缓存已清除')


def get_upstream(table_name: str) -> list[str]:
    """返回指定表的所有直接上游依赖（source → table_name 的 source 集合）"""
    lineage = get_lineage()
    table_name = table_name.lower()
    return [
        e.source for e in lineage['edges']
        if e.target == table_name
    ]


def get_downstream(table_name: str) -> list[str]:
    """返回指定表的所有直接下游依赖（table_name → target 的 target 集合）"""
    lineage = get_lineage()
    table_name = table_name.lower()
    return [
        e.target for e in lineage['edges']
        if e.source == table_name
    ]


def get_db_color() -> dict[str, str]:
    """各 db 对应的颜色，供可视化使用"""
    return {
        'ods':           '#3498db',
        'dwd':           '#2ecc71',
        'dws':           '#f39c12',
        'ads':           '#e74c3c',
        'stream':        '#9b59b6',
        'feature_store': '#1abc9c',
        'ml_metadata':   '#e67e22',
    }


# ── 简单调试入口 ──────────────────────────────────────────────
if __name__ == '__main__':
    result = get_lineage()
    print(f'节点数：{len(result["nodes"])}')
    print(f'边数：  {len(result["edges"])}')
    print('\n--- 节点列表 ---')
    for n in sorted(result['nodes'], key=lambda x: x.name):
        print(f'  [{n.node_type:5s}] {n.name}  ({n.source_file})')
    print('\n--- 边列表 ---')
    for e in result['edges']:
        print(f'  {e.source}  --[{e.edge_type}]-->  {e.target}')
