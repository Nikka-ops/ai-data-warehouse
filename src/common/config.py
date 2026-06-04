# -*- coding: utf-8 -*-
"""
配置入口（兼容层）—— 透明再导出项目根 `config.py` 的唯一配置实例。

历史上 src 树曾维护独立的 pydantic 配置，现已统一到根 `config.py`
作为 single source of truth。本模块保留 `from src.common.config import cfg`
的旧导入路径，避免破坏现有调用方。
"""
from config import Settings, cfg

__all__ = ["cfg", "Settings"]
