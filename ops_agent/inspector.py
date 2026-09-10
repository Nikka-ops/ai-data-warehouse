# -*- coding: utf-8 -*-
"""
链路巡检 Agent

把三个探针的原始数据，变成人能一眼看懂的「健康卡」，并对异常做研判。

沿用整个项目一以贯之的成本设计：规则先判、命中才调 LLM。
巡检每分钟都在跑，绝大多数时候链路是健康的 —— 这时候完全不需要大模型，
规则直接出一张全绿的健康卡。只有当探针报出异常（checkpoint 失败、
消费积压、BE 掉线），才把上下文交给 LLM 去研判「这是背压、数据倾斜、
还是资源不足」并给处置建议。正常巡检的 LLM 调用量是零。

对外提供两个入口：
    inspect()            —— 定时巡检，返回健康卡 + 异常研判，可落库
    ask(question)        —— 「问一句看全链路」，自然语言查健康状态

前者给 cron，后者给人。二者共用同一套探针和研判逻辑。
"""

from __future__ import annotations

import os
import json
import uuid
from datetime import datetime

from ops_agent import probes


# ── 健康卡：把探针结果规则化成红黄绿 ──────────────────────────

# emoji 在部分 Windows 控制台（GBK）会编码报错，按输出能力自适应降级成文字标记，
# 保证健康卡在任何环境都能打印/落日志，不会因为一个图标把巡检搞崩。
def _supports_emoji() -> bool:
    import sys
    enc = (getattr(sys.stdout, 'encoding', '') or '').lower()
    return 'utf' in enc


if _supports_emoji():
    _EMOJI = {'OK': '🟢', 'WARN': '🟡', 'CRITICAL': '🔴', 'UNREACHABLE': '⚫'}
else:
    _EMOJI = {'OK': '[OK]', 'WARN': '[WARN]', 'CRITICAL': '[CRIT]', 'UNREACHABLE': '[N/A]'}


def build_health_card(snapshot: dict) -> dict:
    """
    从 probe_all() 的快照生成结构化健康卡。
    纯规则，不调 LLM。
    """
    lines = []
    anomalies = []

    for comp_key, label in (('flink', 'Flink'), ('kafka', 'Kafka'), ('doris', 'Doris')):
        comp = snapshot.get(comp_key, {})
        status = comp.get('status', 'UNREACHABLE')
        icon = _EMOJI.get(status, '⚫')
        lines.append(f'{icon} {label:6} {status:12} {comp.get("detail", "")}')

        # 收集需要研判的异常
        if status in ('CRITICAL', 'WARN', 'UNREACHABLE'):
            anomalies.append(_extract_anomaly(comp_key, label, comp))

    overall = snapshot.get('overall', 'UNREACHABLE')
    return {
        'overall': overall,
        'overall_icon': _EMOJI.get(overall, '⚫'),
        'checked_at': snapshot.get('checked_at'),
        'card_lines': lines,
        'anomalies': [a for a in anomalies if a],
    }


def _extract_anomaly(comp_key: str, label: str, comp: dict) -> dict | None:
    """从组件健康数据里提取「具体哪里出了问题」，供 LLM 研判"""
    status = comp.get('status')
    if status == 'OK':
        return None

    issues = []
    if comp_key == 'flink':
        for job in comp.get('jobs', []):
            for iss in job.get('issues', []):
                issues.append(f"[{job.get('job_name', job.get('job_id'))}] {iss}")
    elif comp_key == 'kafka':
        for g in comp.get('groups', []):
            if g.get('status') != 'OK':
                issues.append(f"[{g['group_id']}] {g.get('detail', '')}")
    elif comp_key == 'doris':
        for k, v in comp.get('checks', {}).items():
            if isinstance(v, dict) and 'issue' in v:
                issues.append(f'[{k}] {v["issue"]}')
    if not issues and comp.get('detail'):
        issues.append(comp['detail'])

    return {
        'component': label,
        'status': status,
        'issues': issues,
        'raw': comp,
    }


# ── LLM 研判：仅对异常调用 ────────────────────────────────────

DIAGNOSE_PROMPT = """你是实时数仓的 SRE 专家。链路巡检发现以下异常，请研判根因方向并给出处置建议。

链路架构：MySQL → Flink CDC → Kafka → Flink 窗口聚合 → Doris

发现的异常：
{anomalies}

请针对每个异常，从这些方向判断可能根因：
- 反压（backpressure）：下游算子处理不过来，积压往上游传导
- 数据倾斜（skew）：某个 key 的数据量远超其他，单个 subtask 被打爆
- 资源不足（resource）：TM 内存/CPU 不够，或 Doris BE 磁盘/CPU 打满
- checkpoint 问题：状态过大、对齐超时、后端存储慢
- 外部依赖：Kafka/Doris 本身故障或网络问题

只输出一行 JSON，格式：
{{"root_cause":"方向","reason":"一句话分析","action":"一句话处置建议"}}"""


def _get_llm():
    from openai import OpenAI
    return OpenAI(
        api_key=os.getenv('DEEPSEEK_API_KEY', ''),
        base_url=os.getenv('DEEPSEEK_API_BASE', 'https://api.deepseek.com'),
        timeout=30.0,
    )


def _ai_enabled() -> bool:
    return bool(os.getenv('DEEPSEEK_API_KEY', '').strip())


def diagnose_anomaly(anomaly: dict) -> dict:
    """
    对单个异常做 LLM 研判。LLM 不可用时降级为规则提示。
    """
    issues_text = '\n'.join(f'- {i}' for i in anomaly.get('issues', []))
    base = {
        'component': anomaly['component'],
        'status': anomaly['status'],
        'issues': anomaly.get('issues', []),
    }

    if not _ai_enabled():
        base.update({
            'root_cause': '未研判',
            'reason': '未配置 DEEPSEEK_API_KEY，仅规则告警',
            'action': _rule_hint(anomaly),
            'llm_called': 0,
        })
        return base

    try:
        resp = _get_llm().chat.completions.create(
            model=os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'),
            messages=[{'role': 'user',
                       'content': DIAGNOSE_PROMPT.format(anomalies=issues_text)}],
            temperature=0.2,
            max_tokens=200,
        )
        content = (resp.choices[0].message.content or '').strip()
        content = content.replace('```json', '').replace('```', '').strip()
        obj = json.loads(content)
        base.update({
            'root_cause': obj.get('root_cause', '未知'),
            'reason': obj.get('reason', ''),
            'action': obj.get('action', ''),
            'llm_called': 1,
        })
    except Exception as e:
        base.update({
            'root_cause': '研判失败',
            'reason': f'LLM 不可用：{str(e)[:100]}',
            'action': _rule_hint(anomaly),
            'llm_called': 0,
        })
    return base


def _rule_hint(anomaly: dict) -> str:
    """LLM 不可用时的兜底建议，按组件给方向"""
    comp = anomaly['component']
    return {
        'Flink': '查 Flink UI 的 Backpressure 与 Checkpoint 页签，定位瓶颈算子',
        'Kafka': '对比消费速率与生产速率，确认是下游处理慢还是消费组掉线',
        'Doris': '检查 BE 存活与磁盘水位，查看导入报错详情',
    }.get(comp, '人工介入排查')


# ── 巡检入口 ──────────────────────────────────────────────────

def inspect(persist: bool = False) -> dict:
    """
    定时巡检：探测全链路 → 生成健康卡 → 异常研判。

    persist=True 时把异常研判结果落到 stream.ai_quality_alerts。
    """
    snapshot = probes.probe_all()
    card = build_health_card(snapshot)

    diagnoses = []
    for anomaly in card['anomalies']:
        diagnoses.append(diagnose_anomaly(anomaly))
    card['diagnoses'] = diagnoses

    if persist and diagnoses:
        _persist_alerts(diagnoses, snapshot)

    return card


def _persist_alerts(diagnoses: list[dict], snapshot: dict) -> None:
    """把研判结果写入告警表，复用 stream.ai_quality_alerts"""
    try:
        from common.doris_client import get_client
        doris = get_client()
        now = datetime.now()
        rows = []
        for d in diagnoses:
            detail = '; '.join(d.get('issues', []))[:1000]
            suggestion = f"[{d.get('root_cause')}] {d.get('reason')} → {d.get('action')}"
            rows.append([
                now.date(),
                f"ops_{d['component']}_{now:%Y%m%d%H%M}",
                now,
                'LINK_HEALTH',
                'HIGH' if d['status'] == 'CRITICAL' else 'MEDIUM',
                d['component'],
                'link_health',
                detail,
                suggestion[:2000],
                now, now, 0, 0, 0,
                int(d.get('llm_called', 0)),
            ])
        doris.insert(
            'stream.ai_quality_alerts', rows,
            column_names=[
                'alert_date', 'alert_key', 'alert_time', 'alert_type', 'severity',
                'table_name', 'field_name', 'detail', 'ai_suggestion',
                'window_start', 'window_end', 'metric_value', 'baseline_value',
                'threshold_value', 'llm_called',
            ])
    except Exception as e:
        print(f'[巡检] 告警落库失败（不影响巡检本身）：{e}')


def format_card(card: dict) -> str:
    """把健康卡渲染成一段可读文本，给控制台/推送用"""
    out = [
        f"{card['overall_icon']} 实时链路健康巡检　{card['checked_at']}　整体：{card['overall']}",
        '─' * 50,
    ]
    out.extend(card['card_lines'])
    if card.get('diagnoses'):
        out.append('─' * 50)
        out.append('异常研判：')
        for d in card['diagnoses']:
            out.append(f"  {_EMOJI.get(d['status'])} {d['component']} · {d.get('root_cause', '')}")
            if d.get('reason'):
                out.append(f"     原因：{d['reason']}")
            if d.get('action'):
                out.append(f"     处置：{d['action']}")
    else:
        out.append('全链路正常，无需研判（本次 LLM 调用 0 次）')
    return '\n'.join(out)


# ── 「问一句看全链路」──────────────────────────────────────────

def ask(question: str = '现在链路健康吗') -> str:
    """
    自然语言入口。当前实现是「先巡检、再把健康卡连同问题交给 LLM 组织回答」，
    让用户不用逐个打开 Flink UI / Kafka UI / Doris 控制台。
    """
    card = inspect(persist=False)
    card_text = format_card(card)

    if not _ai_enabled():
        return card_text  # 无 LLM 时直接返回结构化健康卡

    try:
        prompt = f"""用户问：{question}

以下是刚刚采集的实时链路健康巡检结果：

{card_text}

请用中文简洁回答用户的问题，重点说清楚：整体是否健康、有没有需要立刻处理的问题、
如果有异常给出最关键的一条处置建议。不要罗列所有细节，像一个运维同事口头汇报那样说人话。"""
        resp = _get_llm().chat.completions.create(
            model=os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'),
            messages=[{'role': 'user', 'content': prompt}],
            temperature=0.3,
            max_tokens=400,
        )
        answer = (resp.choices[0].message.content or '').strip()
        return f'{answer}\n\n{"─"*50}\n{card_text}'
    except Exception:
        return card_text


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        print(ask(' '.join(sys.argv[1:])))
    else:
        print(format_card(inspect(persist=False)))
