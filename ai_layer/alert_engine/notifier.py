# -*- coding: utf-8 -*-
"""
告警通知器：支持飞书/钉钉/Slack/通用 Webhook 三种格式。
WEBHOOK_URL 从环境变量读取，为空则跳过推送。
"""
import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from utils.logger import get_logger

log = get_logger('alert_engine.notifier')

# 严重程度颜色映射（飞书）
_SEVERITY_COLOR = {
    'P1': 'red',
    'P2': 'orange',
    'P3': 'yellow',
    'P4': 'blue',
}

# 严重程度颜色映射（Slack hex）
_SEVERITY_HEX = {
    'P1': '#FF0000',
    'P2': '#FF8C00',
    'P3': '#FFD700',
    'P4': '#1E90FF',
}


def _detect_webhook_type(url: str) -> str:
    """根据 URL 特征判断类型：feishu/dingtalk/slack/generic"""
    url_lower = url.lower()
    if 'feishu.cn' in url_lower or 'larkoffice.com' in url_lower or 'larksuite.com' in url_lower:
        return 'feishu'
    if 'dingtalk.com' in url_lower or 'oapi.dingtalk' in url_lower:
        return 'dingtalk'
    if 'hooks.slack.com' in url_lower or 'slack.com' in url_lower:
        return 'slack'
    return 'generic'


def _fmt_time(dt) -> str:
    """格式化时间，兼容 datetime 对象和字符串"""
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    return str(dt)


def _build_feishu_card(alert, decision: dict) -> dict:
    """
    飞书卡片格式（card）：
    - 标题：[{severity}] {title}
    - 字段：指标值、影响表、触发时间
    - 决策：执行的动作、结果
    - 颜色：P1=red, P2=orange, P3=yellow, P4=blue
    """
    color = _SEVERITY_COLOR.get(alert.severity, 'blue')
    header_color_map = {
        'red': 'red',
        'orange': 'orange',
        'yellow': 'yellow',
        'blue': 'blue',
    }
    template = header_color_map.get(color, 'blue')

    affected = ', '.join(alert.affected_tables[:5]) if alert.affected_tables else '无'
    downstream = ', '.join(alert.downstream_tables[:5]) if alert.downstream_tables else '无'
    fired_at = _fmt_time(alert.fired_at)

    action_result = (
        f"动作: {decision.get('action', '无')}\n"
        f"技能: {decision.get('skill', '无')}\n"
        f"结果: {decision.get('result', '无')}\n"
        f"成功: {'是' if decision.get('success') else '否'}"
    )

    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"[{alert.severity}] {alert.title}",
                },
                "template": template,
            },
            "elements": [
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**告警来源**\n{alert.source}",
                            },
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**类别**\n{alert.category}",
                            },
                        },
                    ],
                },
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**指标名称**\n{alert.metric_name}",
                            },
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": (
                                    f"**当前值 / 阈值**\n"
                                    f"{alert.current_value} / {alert.threshold_value}"
                                ),
                            },
                        },
                    ],
                },
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": False,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**受影响表**\n{affected}",
                            },
                        },
                    ],
                },
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": False,
                            "text": {
                                "tag": "lark_md",
                                "content": f"**下游影响表**\n{downstream}",
                            },
                        },
                    ],
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**Agent 决策**\n{action_result}",
                    },
                },
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": f"触发时间: {fired_at}  |  告警ID: {alert.alert_id}",
                        }
                    ],
                },
            ],
        },
    }
    return card


def _build_dingtalk_card(alert, decision: dict) -> dict:
    """钉钉 actionCard 格式，带"查看详情"按钮"""
    fired_at = _fmt_time(alert.fired_at)
    affected = ', '.join(alert.affected_tables[:5]) if alert.affected_tables else '无'

    text = (
        f"## [{alert.severity}] {alert.title}\n\n"
        f"- **来源**: {alert.source}\n"
        f"- **类别**: {alert.category}\n"
        f"- **指标**: {alert.metric_name}\n"
        f"- **当前值**: {alert.current_value}  **阈值**: {alert.threshold_value}\n"
        f"- **受影响表**: {affected}\n"
        f"- **触发时间**: {fired_at}\n\n"
        f"---\n\n"
        f"**Agent 决策**\n\n"
        f"- 技能: {decision.get('skill', '无')}\n"
        f"- 动作: {decision.get('action', '无')}\n"
        f"- 结果: {decision.get('result', '无')}\n"
        f"- 成功: {'是' if decision.get('success') else '否'}\n"
    )

    return {
        "msgtype": "actionCard",
        "actionCard": {
            "title": f"[{alert.severity}] {alert.title}",
            "text": text,
            "btnOrientation": "0",
            "btns": [
                {
                    "title": "查看详情",
                    "actionURL": "dingtalk://dingtalkclient/page/link?url=about:blank&pc_slide=false",
                }
            ],
        },
    }


def _build_slack_blocks(alert, decision: dict) -> dict:
    """Slack Block Kit 格式"""
    color = _SEVERITY_HEX.get(alert.severity, '#1E90FF')
    fired_at = _fmt_time(alert.fired_at)
    affected = ', '.join(alert.affected_tables[:5]) if alert.affected_tables else '无'
    downstream = ', '.join(alert.downstream_tables[:5]) if alert.downstream_tables else '无'

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"[{alert.severity}] {alert.title}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*来源*\n{alert.source}"},
                {"type": "mrkdwn", "text": f"*类别*\n{alert.category}"},
                {"type": "mrkdwn", "text": f"*指标*\n{alert.metric_name}"},
                {"type": "mrkdwn", "text": f"*当前值 / 阈值*\n{alert.current_value} / {alert.threshold_value}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*受影响表*\n{affected}"},
                {"type": "mrkdwn", "text": f"*下游影响表*\n{downstream}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Agent 决策*\n"
                    f"技能: `{decision.get('skill', '无')}`  "
                    f"动作: `{decision.get('action', '无')}`  "
                    f"成功: {'✅' if decision.get('success') else '❌'}\n"
                    f"结果: {decision.get('result', '无')}"
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"触发时间: {fired_at}  |  告警ID: `{alert.alert_id}`",
                }
            ],
        },
    ]

    return {
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ]
    }


def _build_generic(alert, decision: dict) -> dict:
    """通用格式：{"text": "..."}"""
    fired_at = _fmt_time(alert.fired_at)
    affected = ', '.join(alert.affected_tables) if alert.affected_tables else '无'

    text = (
        f"[{alert.severity}] {alert.title}\n"
        f"来源: {alert.source} | 类别: {alert.category}\n"
        f"指标: {alert.metric_name} | 当前值: {alert.current_value} | 阈值: {alert.threshold_value}\n"
        f"受影响表: {affected}\n"
        f"触发时间: {fired_at}\n"
        f"Agent决策 - 技能: {decision.get('skill', '无')} | 动作: {decision.get('action', '无')} | "
        f"成功: {'是' if decision.get('success') else '否'} | 结果: {decision.get('result', '无')}"
    )
    return {"text": text}


def notify(alert, decision: dict):
    """
    根据 WEBHOOK_URL 自动选择格式发送。
    decision 是 orchestrator 的处置结果：
    {"skill": str, "action": str, "result": str, "success": bool}
    失败只记 warning，不 raise。
    """
    webhook_url = os.getenv('WEBHOOK_URL', '').strip()
    if not webhook_url:
        log.debug("WEBHOOK_URL 未配置，跳过通知推送")
        return

    try:
        import urllib.request

        webhook_type = _detect_webhook_type(webhook_url)
        log.info("发送告警通知 type=%s alert_id=%s", webhook_type, alert.alert_id)

        if webhook_type == 'feishu':
            payload = _build_feishu_card(alert, decision)
        elif webhook_type == 'dingtalk':
            payload = _build_dingtalk_card(alert, decision)
        elif webhook_type == 'slack':
            payload = _build_slack_blocks(alert, decision)
        else:
            payload = _build_generic(alert, decision)

        data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={'Content-Type': 'application/json; charset=utf-8'},
            method='POST',
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp_body = resp.read().decode('utf-8', errors='replace')
            log.info(
                "通知发送成功 type=%s status=%d alert_id=%s",
                webhook_type, resp.status, alert.alert_id,
            )
            log.debug("Webhook 响应: %s", resp_body[:200])

    except Exception as e:
        log.warning(
            "通知发送失败 alert_id=%s: %s",
            getattr(alert, 'alert_id', '?'), e,
        )
