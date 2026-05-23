# -*- coding: utf-8 -*-
"""监控 Dashboard 入口（主要逻辑在 app/dashboard.py）"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

# 直接重导出现有 dashboard
from app.dashboard import *  # noqa: F401, F403
