# -*- coding: utf-8 -*-
"""SQL 安全校验工具（全项目共用）"""
import re

_FORBIDDEN = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE']


def validate_sql(sql: str) -> None:
    """校验 SQL 安全性；不合法时抛出 ValueError。"""
    upper = sql.strip().upper()
    for kw in _FORBIDDEN:
        if re.search(rf'\b{kw}\b', upper):
            raise ValueError(f'不允许执行 {kw} 操作')
    if not (upper.startswith('SELECT') or upper.startswith('WITH')):
        raise ValueError('SQL 必须以 SELECT 或 WITH 开头')


def check_sql(sql: str) -> str | None:
    """校验 SQL 安全性；不合法时返回错误字符串，合法时返回 None。"""
    try:
        validate_sql(sql)
        return None
    except ValueError as e:
        return f'错误：{e}'
