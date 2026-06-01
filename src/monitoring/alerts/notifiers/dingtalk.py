import json
import urllib.request

def send_dingtalk(webhook_url: str, title: str, content: str, severity: str = "P3") -> bool:
    """发送钉钉 Markdown 消息"""
    color = {"P1": "#FF0000", "P2": "#FF8C00", "P3": "#FFA500", "P4": "#808080"}.get(severity, "#808080")
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "title": f"[{severity}] {title}",
            "text": f"## [{severity}] {title}\n\n{content}\n\n<font color={color}>严重程度：{severity}</font>",
        }
    }
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False
