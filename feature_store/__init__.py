# Feature Store — AI数仓核心模块
from .feast_store import FeastStore  # Feast 重构入口，替代旧版 registry.py

__all__ = ["FeastStore"]
