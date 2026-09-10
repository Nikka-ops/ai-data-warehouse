# -*- coding: utf-8 -*-
"""公共模块：Doris 访问层"""

from common.doris_client import DorisClient, QueryResult, get_client

__all__ = ['DorisClient', 'QueryResult', 'get_client']
