# -*- coding: utf-8 -*-
"""通知工具"""
from datetime import datetime

from langchain_core.tools import tool


@tool
def send_alert_notification(title: str, content: str, severity: str = "P3") -> str:
    """发送告警通知到配置的 Webhook（DingTalk/Feishu/Slack）"""
    try:
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
        from ai_layer.alert_engine.notifier import notify

        class _Alert:
            alert_id: str = ""
            source: str = "agent"
            affected_tables: list = []
            downstream_tables: list = []

            def __init__(self, t: str, s: str) -> None:
                self.title = t
                self.severity = s
                self.fired_at = datetime.now()

        notify(_Alert(title, severity), {
            "alert_id": "",
            "skill": "notification",
            "action": "send_alert",
            "result": content[:200],
            "success": True,
            "report": content,
        })
        return f"通知已发送：{title}"
    except Exception as e:
        return f"通知发送失败: {e}"
