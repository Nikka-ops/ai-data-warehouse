# -*- coding: utf-8 -*-
"""通知工具"""
from langchain_core.tools import tool


@tool
def send_alert_notification(title: str, content: str, severity: str = "P3") -> str:
    """发送告警通知到配置的 Webhook（DingTalk/Feishu/Slack）"""
    try:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
        from ai_layer.alert_engine.notifier import send_notification
        send_notification(title=title, content=content, severity=severity)
        return f"通知已发送：{title}"
    except Exception as e:
        return f"通知发送失败: {e}"
