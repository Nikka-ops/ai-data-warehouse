# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from langchain_core.tools import BaseTool as LCBaseTool

class BaseTool(ABC):
    """Tool 抽象基类，用于文档和类型检查"""
    name: str
    description: str

    @abstractmethod
    def _run(self, *args, **kwargs): ...
