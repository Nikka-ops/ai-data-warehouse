# -*- coding: utf-8 -*-
"""
通用工具函数集合
整合 utils/logger.py 的日志工厂和 utils/retry.py 的重试装饰器
"""

from __future__ import annotations

import logging
import os
import sys
import time
import functools
from typing import Callable, Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


# ── 日志工厂（复用 utils/logger.py 逻辑）────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """获取带格式化输出的全项目统一 Logger"""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # 避免重复添加 handler

    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, level, logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ── 重试装饰器 ────────────────────────────────────────────────────────

_retry_log = get_logger("utils.retry")


def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """
    指数退避重试装饰器
    max_attempts: 最大重试次数（含首次调用）
    base_delay:   首次重试等待秒数，后续翻倍
    exceptions:   触发重试的异常类型
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        delay = base_delay * (2 ** (attempt - 1))  # 指数退避
                        _retry_log.warning(
                            "%s 第 %d/%d 次失败，%.1f 秒后重试：%s",
                            func.__name__, attempt, max_attempts, delay, exc,
                        )
                        time.sleep(delay)
            raise last_exc  # type: ignore[misc]
        return wrapper  # type: ignore[return-value]
    return decorator


# ── 数学工具 ──────────────────────────────────────────────────────────

def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """安全除法，除数为零时返回 default"""
    if b == 0:
        return default
    return a / b


# ── 字符串工具 ────────────────────────────────────────────────────────

def truncate_str(s: str, max_len: int = 200) -> str:
    """截断超长字符串，末尾加省略号"""
    if len(s) <= max_len:
        return s
    return s[:max_len] + "..."
