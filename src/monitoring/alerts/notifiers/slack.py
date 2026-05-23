import json, urllib.request

def send_slack(webhook_url: str, title: str, content: str, severity: str = "P3") -> bool:
    """发送 Slack Block Kit 消息"""
    emoji = {"P1": ":red_circle:", "P2": ":orange_circle:", "P3": ":yellow_circle:", "P4": ":white_circle:"}
    payload = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": f"{emoji.get(severity, '')} [{severity}] {title}"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": content}},
        ]
    }
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False
