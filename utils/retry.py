# -*- coding: utf-8 -*-
"""重试与熔断工具，基于 tenacity"""
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, before_sleep_log, RetryError,
)
import logging

_log = logging.getLogger('retry')

# LLM API 调用重试：最多3次，指数退避 2~30s
llm_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(_log, logging.WARNING),
    reraise=True,
)

# ClickHouse 操作重试：最多4次，指数退避 1~15s
ch_retry = retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(_log, logging.WARNING),
    reraise=True,
)

__all__ = ['llm_retry', 'ch_retry', 'RetryError']
