#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 告警自动处置服务（Kappa 架构增强版）

检测范围：
  1. 数据质量告警   — stream.ai_quality_alerts（Flink 写入）
  2. Kappa 回放失败 — stream.kappa_replay_jobs status=failed
  3. Kafka Lag 爆发 — stream.kappa_consumer_lag lag 突增
  4. ETL 质量劣化   — stream.etl_audit_log quality_score 下降

处置流程：
  检测 → 优先级排序 → 上下文采集 → LLM 根因分析
  → 执行修复 Playbook → 写审计记录 → 反馈通知

运行：
  python ai_layer/alert_investigator.py --loop 30
  python ai_layer/alert_investigator.py           # 单次
"""
import os, sys, json, uuid, time, re, argparse, subprocess
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry, llm_retry

log = get_logger('alert_investigator')

# 内存去重（alert_id → 处置时间），避免重复处理同一告警
_HANDLED: dict[str, datetime] = {}
_HANDLED_TTL_MINUTES = 30


# ── 阈值配置 ──────────────────────────────────────────────────
KAFKA_LAG_HIGH  = 50_000    # 条，超过此值为 HIGH
KAFKA_LAG_CRIT  = 200_000   # 条，超过此值为 CRITICAL
ETL_SCORE_WARN  = 70.0      # 低于此分触发 MEDIUM 告警
ETL_SCORE_HIGH  = 50.0      # 低于此分触发 HIGH 告警


# ─────────────────────────────────────────────────────────────
# LLM Prompt
# ─────────────────────────────────────────────────────────────

_PROMPT = """你是生产环境数据仓库的值班 SRE，负责自动处置流数据管道告警。

【告警信息】
{alert_info}

【系统上下文快照】
{context}

请分析并严格输出以下 JSON（禁止输出 JSON 以外的内容）：
{{
  "root_cause": "根本原因（1-2句，要具体指出是哪个组件、哪种数据、哪个时段）",
  "impact_scope": "影响范围（列举受影响的表/服务/时间段）",
  "action_type": "RESTART_REPLAY | TRIGGER_ETL | QUARANTINE_WINDOW | SCALE_CONSUMER | NOTIFY_ONLY | NOOP",
  "action_detail": "动作参数（例如：回放起点时间、ETL 扫描范围、隔离时间窗口等）",
  "confidence": 0.85,
  "status": "resolved | monitoring | escalated",
  "next_check_minutes": 5
}}

action_type 选择规则：
- RESTART_REPLAY：Kappa 回放失败或历史数据有空洞
- TRIGGER_ETL：ETL 质量分下降或字段异常率上升
- QUARANTINE_WINDOW：某时间窗口数据严重异常（取消率>40%、价格错误等）
- SCALE_CONSUMER：Kafka lag 持续积压
- NOTIFY_ONLY：问题已定位但无法自动修复，需人工介入
- NOOP：告警属于短暂波动，无需操作

status 规则：
- resolved：已执行动作且预期能修复
- monitoring：已记录，等待下次巡检确认恢复
- escalated：超出自动处置能力，已记录待人工
"""


# ─────────────────────────────────────────────────────────────
# ClickHouse 连接
# ─────────────────────────────────────────────────────────────

@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


def _safe_query(ch, sql: str, default=None):
    try:
        return ch.query(sql).result_rows
    except Exception as e:
        log.debug('查询失败（%s）：%s', sql[:60], e)
        return default if default is not None else []


# ─────────────────────────────────────────────────────────────
# 告警检测
# ─────────────────────────────────────────────────────────────

def _fetch_data_quality_alerts(ch) -> list[dict]:
    """从 stream.ai_quality_alerts 拉取最近20分钟未处理的数据质量告警"""
    rows = _safe_query(ch, """
        SELECT alert_id, alert_time, alert_type, severity, table_name,
               field_name, detail, ai_suggestion, metric_value, threshold_value
        FROM stream.ai_quality_alerts
        WHERE alert_time >= now() - INTERVAL 20 MINUTE
        ORDER BY severity DESC, alert_time DESC
        LIMIT 30
    """)
    result = []
    for r in rows:
        aid = str(r[0]) if r[0] else str(uuid.uuid4())
        if _is_handled(aid):
            continue
        result.append({
            'alert_id': aid, 'alert_time': r[1],
            'alert_type': str(r[2]), 'severity': str(r[3]),
            'source': 'data_quality', 'category': 'data_quality',
            'title': str(r[6])[:100],
            'detail': str(r[6]),
            'metric_value': float(r[8] or 0),
            'threshold_value': float(r[9] or 0),
            'context_hint': {'table': r[4], 'field': r[5], 'ai_suggestion': r[7]},
        })
    return result


def _detect_kappa_replay_failures(ch) -> list[dict]:
    """检测最近30分钟内失败的 Kappa 回放任务"""
    rows = _safe_query(ch, """
        SELECT job_id, job_name, start_time, error_msg, records_processed
        FROM stream.kappa_replay_jobs
        WHERE status = 'failed'
          AND start_time >= now() - INTERVAL 30 MINUTE
    """)
    result = []
    for r in rows:
        aid = f'replay_fail_{r[0][:12]}'
        if _is_handled(aid):
            continue
        result.append({
            'alert_id': aid,
            'alert_time': r[2] or datetime.now(),
            'alert_type': 'KAPPA_REPLAY',
            'severity': 'HIGH',
            'source': 'kappa_replay',
            'category': 'system',
            'title': f'Kappa 回放任务失败：{r[1]}',
            'detail': f"任务 {r[1]}（{r[0][:8]}）执行失败。已处理 {r[4]:,} 条。错误：{str(r[3])[:200]}",
            'metric_value': float(r[4] or 0),
            'threshold_value': 0,
            'context_hint': {'job_id': str(r[0]), 'job_name': str(r[1])},
        })
    return result


def _detect_kafka_lag(ch) -> list[dict]:
    """检测 Kafka 消费者 Lag 突增"""
    rows = _safe_query(ch, """
        SELECT consumer_group, topic, max(lag) AS max_lag, avg(lag) AS avg_lag
        FROM stream.kappa_consumer_lag
        WHERE check_time >= now() - INTERVAL 5 MINUTE
          AND is_replay = 0
        GROUP BY consumer_group, topic
        HAVING max_lag > %(threshold)s
    """ % {'threshold': KAFKA_LAG_HIGH})
    result = []
    for r in rows:
        max_lag = int(r[2] or 0)
        severity = 'CRITICAL' if max_lag > KAFKA_LAG_CRIT else 'HIGH'
        aid = f'lag_{r[0]}_{r[1]}_{int(time.time() // 300)}'
        if _is_handled(aid):
            continue
        result.append({
            'alert_id': aid,
            'alert_time': datetime.now(),
            'alert_type': 'KAFKA_LAG',
            'severity': severity,
            'source': 'kafka_consumer',
            'category': 'system',
            'title': f'Kafka Lag 过高：{r[1]} max={max_lag:,}',
            'detail': f"消费组 {r[0]} 主题 {r[1]} Lag 峰值 {max_lag:,}，均值 {int(r[3] or 0):,}",
            'metric_value': float(max_lag),
            'threshold_value': float(KAFKA_LAG_HIGH),
            'context_hint': {'consumer_group': r[0], 'topic': r[1]},
        })
    return result


def _detect_etl_degradation(ch) -> list[dict]:
    """检测 ETL 质量分下降"""
    rows = _safe_query(ch, """
        SELECT round(avg(quality_score), 1) AS avg_score, count() AS runs
        FROM stream.etl_audit_log
        WHERE run_time >= now() - INTERVAL 30 MINUTE
    """)
    result = []
    if rows and rows[0][1] and int(rows[0][1]) > 0:
        avg_score = float(rows[0][0] or 100)
        runs = int(rows[0][1])
        if avg_score < ETL_SCORE_WARN:
            severity = 'HIGH' if avg_score < ETL_SCORE_HIGH else 'MEDIUM'
            aid = f'etl_quality_{int(time.time() // 300)}'
            if not _is_handled(aid):
                result.append({
                    'alert_id': aid,
                    'alert_time': datetime.now(),
                    'alert_type': 'ETL_QUALITY',
                    'severity': severity,
                    'source': 'etl_agent',
                    'category': 'system',
                    'title': f'ETL 质量分下降：{avg_score:.1f}/100（{runs} 次运行）',
                    'detail': f"近30分钟 ETL 平均质量分 {avg_score:.1f}，低于阈值 {ETL_SCORE_WARN}。共 {runs} 次运行。",
                    'metric_value': avg_score,
                    'threshold_value': ETL_SCORE_WARN,
                    'context_hint': {'avg_score': avg_score, 'run_count': runs},
                })
    return result


def _write_system_alert(ch, alert: dict):
    """将系统级告警写入 stream.system_alerts（用于 Superset 监控）"""
    if alert['category'] != 'system':
        return
    try:
        ch.insert(
            'stream.system_alerts',
            [[alert['alert_id'], alert['alert_time'],
              alert['alert_type'], alert['severity'],
              alert['source'], alert['title'], alert['detail'],
              alert['metric_value'], alert['threshold_value'],
              json.dumps(alert.get('context_hint', {}), ensure_ascii=False, default=str),
              0]],
            column_names=['alert_id', 'alert_time', 'alert_type', 'severity',
                          'source', 'title', 'detail', 'metric_value', 'threshold_value',
                          'context_json', 'handled'],
        )
    except Exception as e:
        log.debug('写 system_alert 失败：%s', e)


# ─────────────────────────────────────────────────────────────
# 上下文采集
# ─────────────────────────────────────────────────────────────

def _collect_context(ch, alert: dict) -> dict:
    ctx = {'alert_type': alert['alert_type'], 'hint': alert.get('context_hint', {})}

    # 通用：最近5个分钟窗口
    rows = _safe_query(ch, """
        SELECT window_start, order_cnt, total_gmv, avg_price, top_category
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL 20 MINUTE
        ORDER BY window_start DESC LIMIT 8
    """)
    ctx['recent_windows'] = [
        {'t': str(r[0]), 'orders': r[1], 'gmv': round(float(r[2]), 0),
         'avg_price': round(float(r[3]), 2), 'top_cat': r[4]}
        for r in rows
    ]

    # 通用：近5分钟取消率
    rows = _safe_query(ch, """
        SELECT countIf(order_status='canceled'), count()
        FROM ods.orders_stream
        WHERE event_time >= now() - INTERVAL 5 MINUTE
    """)
    if rows and rows[0][1]:
        c, t = int(rows[0][0]), int(rows[0][1])
        ctx['cancel_rate_5min'] = {'canceled': c, 'total': t,
                                   'rate': round(c / t, 4) if t else 0}

    # 告警类型专项上下文
    atype = alert['alert_type']

    if atype == 'KAPPA_REPLAY':
        rows = _safe_query(ch, """
            SELECT job_name, start_time, end_time, records_processed, status, error_msg
            FROM stream.kappa_replay_jobs
            ORDER BY start_time DESC LIMIT 5
        """)
        ctx['recent_replay_jobs'] = [
            {'name': r[0], 'start': str(r[1]), 'end': str(r[2]),
             'records': r[3], 'status': r[4], 'error': str(r[5])[:100]}
            for r in rows
        ]

    elif atype == 'KAFKA_LAG':
        rows = _safe_query(ch, """
            SELECT check_time, consumer_group, topic, lag, throughput_per_s
            FROM stream.kappa_consumer_lag
            WHERE check_time >= now() - INTERVAL 30 MINUTE
            ORDER BY check_time DESC LIMIT 10
        """)
        ctx['lag_history'] = [
            {'t': str(r[0]), 'group': r[1], 'topic': r[2],
             'lag': r[3], 'throughput': round(float(r[4] or 0), 1)}
            for r in rows
        ]

    elif atype == 'ETL_QUALITY':
        rows = _safe_query(ch, """
            SELECT run_time, quality_score, issues_found, records_fixed, status, summary
            FROM stream.etl_audit_log
            ORDER BY run_time DESC LIMIT 5
        """)
        ctx['etl_recent_runs'] = [
            {'t': str(r[0]), 'score': float(r[1] or 0),
             'issues': r[2], 'fixed': r[3], 'status': r[4], 'summary': str(r[5])[:100]}
            for r in rows
        ]

    elif atype == 'QUALITY':
        rows = _safe_query(ch, """
            SELECT product_category, count(), round(avg(price), 2),
                   countIf(order_status='canceled')
            FROM ods.orders_stream
            WHERE event_time >= now() - INTERVAL 10 MINUTE
            GROUP BY product_category ORDER BY count() DESC LIMIT 8
        """)
        ctx['category_breakdown'] = [
            {'cat': r[0], 'cnt': r[1], 'avg_price': r[2], 'canceled': r[3]}
            for r in rows
        ]

    # Kappa 历史覆盖快照
    rows = _safe_query(ch, """
        SELECT count() AS hrs, min(hour_start), max(hour_start)
        FROM dws.kappa_hourly_agg
    """)
    if rows and rows[0][0]:
        ctx['kappa_coverage'] = {
            'hours': int(rows[0][0]),
            'earliest': str(rows[0][1]),
            'latest': str(rows[0][2]),
        }

    return ctx


# ─────────────────────────────────────────────────────────────
# LLM 分析
# ─────────────────────────────────────────────────────────────

@llm_retry
def _llm_analyze(alert: dict, context: dict) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=45.0)

    alert_info = json.dumps({
        'type': alert['alert_type'], 'severity': alert['severity'],
        'title': alert['title'], 'detail': alert['detail'],
        'metric_value': alert['metric_value'],
        'threshold_value': alert['threshold_value'],
        'time': str(alert['alert_time']),
    }, ensure_ascii=False, indent=2)

    resp = client.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': _PROMPT.format(
            alert_info=alert_info,
            context=json.dumps(context, ensure_ascii=False, indent=2, default=str),
        )}],
        temperature=0.15,
        max_tokens=700,
    )
    raw = resp.choices[0].message.content.strip()
    match = re.search(r'\{.*\}', raw, re.DOTALL)
    if not match:
        raise ValueError(f'LLM 未返回有效 JSON，原文：{raw[:200]}')
    result = json.loads(match.group())
    # 字段保底
    result.setdefault('root_cause', '未能分析根因')
    result.setdefault('impact_scope', '未知')
    result.setdefault('action_type', 'NOTIFY_ONLY')
    result.setdefault('action_detail', '无')
    result.setdefault('confidence', 0.5)
    result.setdefault('status', 'monitoring')
    result.setdefault('next_check_minutes', 10)
    return result


# ─────────────────────────────────────────────────────────────
# 修复 Playbook（每种 action_type 的实际执行逻辑）
# ─────────────────────────────────────────────────────────────

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')


def _play_restart_replay(ch, alert: dict, analysis: dict) -> tuple[bool, str]:
    """重新提交 Kappa 回放任务（写 pending 记录，flink-replay 服务自动拾取）"""
    try:
        job_id   = str(uuid.uuid4())
        job_name = f'auto_retry_{datetime.now().strftime("%Y%m%dT%H%M%S")}'
        ch.insert(
            'stream.kappa_replay_jobs',
            [[job_id, job_name, 'auto_remediation', 'earliest',
              None, None, datetime.now(), None, 0, 'pending',
              '', f'告警自动触发：{alert["title"][:80]}']],
            column_names=['job_id', 'job_name', 'triggered_by', 'from_offset',
                          'replay_from_time', 'replay_until_time',
                          'start_time', 'end_time', 'records_processed',
                          'status', 'error_msg', 'notes'],
        )
        # 如果 flink-replay 服务未运行，也尝试直接启动（后台子进程）
        try:
            subprocess.Popen(
                [sys.executable, 'flink/flink_stream_job.py',
                 '--mode', 'python', '--replay', '--job-name', job_name],
                cwd=_PROJECT_ROOT,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            return True, f'新回放任务已提交并启动：job_id={job_id[:8]}，job_name={job_name}'
        except Exception:
            return True, f'新回放任务已写入队列（job_id={job_id[:8]}），等待 flink-replay 服务拾取'
    except Exception as e:
        return False, f'提交回放任务失败：{e}'


def _play_trigger_etl(ch, alert: dict, analysis: dict) -> tuple[bool, str]:
    """触发 AI ETL Agent 立即执行一轮扫描（子进程，不阻塞）"""
    try:
        proc = subprocess.Popen(
            [sys.executable, 'ai_etl/ai_etl_agent.py', '--lookback', '10'],
            cwd=_PROJECT_ROOT,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return True, f'ETL Agent 已触发重新扫描（pid={proc.pid}），预计30秒内完成'
    except Exception as e:
        return False, f'触发 ETL 失败：{e}'


def _play_quarantine_window(ch, alert: dict, analysis: dict) -> tuple[bool, str]:
    """
    隔离异常时间窗口：在 stream.system_alerts 写一条高优先级标记，
    同时可将异常窗口内的数据写入专用的 quarantine 标记（通过 ai_suggestion 字段）
    """
    try:
        alert_time = alert.get('alert_time', datetime.now())
        win_start  = alert_time if isinstance(alert_time, datetime) else datetime.now()
        win_end    = win_start + timedelta(minutes=1)
        detail = (f"数据隔离：窗口 {win_start.strftime('%H:%M')}~{win_end.strftime('%H:%M')} "
                  f"已标记异常。根因：{analysis['root_cause'][:80]}")
        # 更新告警表 ai_suggestion 字段（ClickHouse 不支持 UPDATE，用新增标记行代替）
        ch.insert(
            'stream.system_alerts',
            [[str(uuid.uuid4()), datetime.now(), 'QUARANTINE', 'HIGH',
              'auto_remediation', f'[隔离] {alert["title"][:80]}',
              detail, 1.0, 1.0, json.dumps({
                  'quarantine_window_start': str(win_start),
                  'quarantine_window_end': str(win_end),
                  'original_alert_id': alert['alert_id'],
              }), 1]],
            column_names=['alert_id', 'alert_time', 'alert_type', 'severity',
                          'source', 'title', 'detail', 'metric_value', 'threshold_value',
                          'context_json', 'handled'],
        )
        return True, f'窗口 {win_start.strftime("%H:%M")} 已隔离标记，数据供人工复核'
    except Exception as e:
        return False, f'隔离失败：{e}'


def _play_scale_consumer(ch, alert: dict, analysis: dict) -> tuple[bool, str]:
    """Kafka lag 过高：记录告警并提示扩容建议（无法自动扩容，给出具体指令）"""
    hint = (
        "建议执行以下操作减少 Lag：\n"
        "  1. docker-compose scale flink-job=2  # 增加 Flink 并行度\n"
        "  2. 或检查 ClickHouse 写入是否成为瓶颈（查看 system.query_log）"
    )
    return True, f'已记录 lag 告警。{hint}'


def _play_notify_only(ch, alert: dict, analysis: dict) -> tuple[bool, str]:
    """只记录不操作，供人工查阅"""
    return True, f'已记录告警，status={analysis["status"]}，需人工确认：{analysis["root_cause"][:100]}'


_PLAYBOOKS = {
    'RESTART_REPLAY':     _play_restart_replay,
    'TRIGGER_ETL':        _play_trigger_etl,
    'QUARANTINE_WINDOW':  _play_quarantine_window,
    'SCALE_CONSUMER':     _play_scale_consumer,
    'NOTIFY_ONLY':        _play_notify_only,
    'NOOP':               lambda ch, a, r: (True, '无需操作，短暂波动'),
}


def _execute_playbook(ch, alert: dict, analysis: dict) -> tuple[str, str, bool]:
    """执行 LLM 决策的 playbook，返回 (action_type, result_msg, success)"""
    atype = analysis.get('action_type', 'NOTIFY_ONLY')
    play  = _PLAYBOOKS.get(atype, _PLAYBOOKS['NOTIFY_ONLY'])
    try:
        success, msg = play(ch, alert, analysis)
        log.info('[Playbook %s] %s → %s', atype, alert['title'][:50], msg[:80])
        return atype, msg, success
    except Exception as e:
        log.error('[Playbook %s] 执行异常：%s', atype, e)
        return atype, f'Playbook 执行异常：{e}', False


# ─────────────────────────────────────────────────────────────
# 审计写入 + 反馈
# ─────────────────────────────────────────────────────────────

def _write_audit(ch, alert: dict, analysis: dict, action_type: str,
                 action_detail: str, action_result: str, action_success: bool,
                 context: dict):
    """写入 stream.remediation_actions 审计表"""
    final_status = analysis.get('status', 'monitoring')
    resolve_time = datetime.now() if final_status == 'resolved' else None
    try:
        ch.insert(
            'stream.remediation_actions',
            [[str(uuid.uuid4()), alert['alert_id'],
              alert['alert_type'], alert['severity'],
              datetime.now(),
              analysis['root_cause'], analysis['impact_scope'],
              float(analysis.get('confidence', 0.5)),
              action_type, action_detail, action_result,
              int(action_success),
              final_status, resolve_time,
              0,
              json.dumps({'alert': alert, 'context': context},
                         ensure_ascii=False, default=str)[:8000],
              ]],
            column_names=['action_id', 'alert_id', 'alert_type', 'alert_severity',
                          'action_time', 'root_cause', 'impact_scope', 'confidence',
                          'action_type', 'action_detail', 'action_result', 'action_success',
                          'final_status', 'resolve_time', 'feedback_sent', 'raw_context'],
        )
    except Exception as e:
        log.error('写审计记录失败：%s', e)


def _emit_feedback(alert: dict, analysis: dict, action_type: str,
                   action_result: str, action_success: bool):
    """输出结构化反馈日志（可对接 Slack/邮件等，当前输出到日志）"""
    status_icon = {'resolved': '✅', 'monitoring': '👁', 'escalated': '🚨'}.get(
        analysis.get('status', ''), '❓')
    success_icon = '✓' if action_success else '✗'

    log.warning(
        '\n'
        '═══════════════════════════════════════\n'
        '  告警处置反馈\n'
        '═══════════════════════════════════════\n'
        '  告警：[%s] %s\n'
        '  根因：%s\n'
        '  影响：%s\n'
        '  动作：%s  %s\n'
        '  结果：%s\n'
        '  状态：%s %s  置信度：%.0f%%\n'
        '═══════════════════════════════════════',
        alert['severity'], alert['title'][:80],
        analysis['root_cause'][:120],
        analysis['impact_scope'][:100],
        action_type, success_icon,
        action_result[:100],
        status_icon, analysis.get('status', ''),
        float(analysis.get('confidence', 0)) * 100,
    )


# ─────────────────────────────────────────────────────────────
# 去重辅助
# ─────────────────────────────────────────────────────────────

def _is_handled(alert_id: str) -> bool:
    now = datetime.now()
    if alert_id in _HANDLED:
        if (now - _HANDLED[alert_id]).total_seconds() < _HANDLED_TTL_MINUTES * 60:
            return True
        del _HANDLED[alert_id]
    return False


def _mark_handled(alert_id: str):
    _HANDLED[alert_id] = datetime.now()
    # 顺手清理过期 key
    expired = [k for k, v in _HANDLED.items()
               if (datetime.now() - v).total_seconds() > _HANDLED_TTL_MINUTES * 60]
    for k in expired:
        del _HANDLED[k]


# ─────────────────────────────────────────────────────────────
# 主处置流程
# ─────────────────────────────────────────────────────────────

_SEVERITY_ORDER = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}


def _collect_all_alerts(ch) -> list[dict]:
    """汇总所有来源的告警，按优先级排序"""
    alerts = []
    alerts.extend(_fetch_data_quality_alerts(ch))
    alerts.extend(_detect_kappa_replay_failures(ch))
    alerts.extend(_detect_kafka_lag(ch))
    alerts.extend(_detect_etl_degradation(ch))
    alerts.sort(key=lambda a: _SEVERITY_ORDER.get(a['severity'], 9))
    return alerts


def handle_alert(ch, alert: dict) -> dict:
    """单条告警完整处置流程：检测 → 分析 → 执行 → 审计 → 反馈"""
    log.info('[处置] [%s/%s] %s', alert['severity'], alert['alert_type'], alert['title'][:70])

    # 1. 系统告警写表
    _write_system_alert(ch, alert)

    # 2. 上下文采集
    context = _collect_context(ch, alert)

    # 3. LLM 根因分析
    try:
        analysis = _llm_analyze(alert, context)
    except Exception as e:
        log.error('[分析失败] %s：%s', alert['alert_id'], e)
        analysis = {
            'root_cause': f'LLM 分析不可用：{e}',
            'impact_scope': '未知',
            'action_type': 'NOTIFY_ONLY',
            'action_detail': '无',
            'confidence': 0.0,
            'status': 'escalated',
            'next_check_minutes': 5,
        }

    # 4. 执行 Playbook
    action_type, action_result, action_success = _execute_playbook(ch, alert, analysis)

    # 5. 写审计
    _write_audit(ch, alert, analysis, action_type,
                 analysis.get('action_detail', ''), action_result, action_success, context)

    # 6. 反馈输出
    _emit_feedback(alert, analysis, action_type, action_result, action_success)

    _mark_handled(alert['alert_id'])
    return {**analysis, 'action_type': action_type, 'action_result': action_result,
            'action_success': action_success}


def run_once() -> dict:
    """单次巡检：返回处置摘要"""
    ch = _get_ch()
    alerts = _collect_all_alerts(ch)
    if not alerts:
        log.debug('无新告警')
        return {'total': 0, 'handled': 0}

    log.info('发现 %d 条告警（按优先级处置）...', len(alerts))
    handled, failed = 0, 0
    stats = defaultdict(int)

    for alert in alerts:
        try:
            result = handle_alert(ch, alert)
            handled += 1
            stats[result.get('status', 'unknown')] += 1
        except Exception as e:
            log.error('处置告警 %s 失败：%s', alert['alert_id'], e)
            failed += 1

    summary = (
        f'本轮处置：{handled} 条（resolved={stats["resolved"]}, '
        f'monitoring={stats["monitoring"]}, escalated={stats["escalated"]}），'
        f'失败 {failed} 条'
    )
    log.info(summary)
    return {'total': len(alerts), 'handled': handled, 'failed': failed, 'stats': dict(stats)}


def run_loop(interval: int = 30):
    log.info('告警自动处置服务启动，间隔 %ds', interval)
    consecutive_errors = 0
    while True:
        try:
            run_once()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            log.error('巡检异常（连续第 %d 次）：%s', consecutive_errors, e)
            if consecutive_errors >= 5:
                log.critical('连续异常 5 次，等待 120s 后重试')
                time.sleep(120)
                consecutive_errors = 0
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description='AI 告警自动处置服务')
    parser.add_argument('--loop', type=int, default=30,
                        help='循环间隔秒数（0=单次执行）')
    args = parser.parse_args()
    if args.loop > 0:
        run_loop(args.loop)
    else:
        summary = run_once()
        print(f"处置完成：{summary}")


if __name__ == '__main__':
    main()
