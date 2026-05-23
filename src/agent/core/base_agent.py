# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any
from src.common.utils import get_logger

class BaseAgent(ABC):
    """所有 Agent 的抽象基类"""
    name: str = "base"

    def __init__(self):
        self.log = get_logger(f'agent.{self.name}')

    @abstractmethod
    def run(self, goal: str) -> dict[str, Any]:
        """执行 Agent 任务，返回 {output: str, intermediate_steps: list}"""
        ...

    def _wrap_result(self, output: str, steps: list = None) -> dict:
        return {"output": output, "intermediate_steps": steps or []}
