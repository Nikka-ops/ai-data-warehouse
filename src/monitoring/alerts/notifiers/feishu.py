import json, urllib.request

def send_feishu(webhook_url: str, title: str, content: str, severity: str = "P3") -> bool:
    """发送飞书卡片消息"""
    color_map = {"P1": "red", "P2": "orange", "P3": "yellow", "P4": "grey"}
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"[{severity}] {title}"},
                "template": color_map.get(severity, "grey"),
            },
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": content}}],
        }
    }
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False
