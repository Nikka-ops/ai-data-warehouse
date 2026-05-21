#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI 告警自动排查与处置服务
轮询 stream.ai_quality_alerts，对新告警执行：
  1. 数据上下文采集（告警时窗口的多维数据）
  2. LLM 根因分析（root cause + 影响范围 + 建议动作）
  3. 自动执行安全操作（触发 ETL 重跑、记录监控标记等）
  4. 写入 stream.alert_investigations

运行：
  python ai_layer/alert_investigator.py          # 单次处理当前待排查告警
  python ai_layer/alert_investigator.py --loop 60 # 每60秒循环
"""
import os, sys, json, uuid, time, argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry, llm_retry

log = get_logger('alert_investigator')

# 已处理的告警 ID（内存去重，重启后重置）
_HANDLED: set[str] = set()

_INVESTIGATION_PROMPT = """你是实时数据仓库的值班工程师，负责自动排查数据质量告警。

【告警信息】
{alert_info}

【告警时段上下文数据】
{context_data}

【Lambda 批实时对账状态】
{reconciliation_info}

请分析并输出 JSON（只输出 JSON，不要其他内容）：
{{
  "root_cause": "根本原因（1-2句话，要具体）",
  "impact_scope": "影响范围（哪些表/业务/时段受影响）",
  "auto_action": "已自动执行的操作（如：触发ETL重扫描、标记异常时窗、无需操作等）",
  "action_result": "操作结果或预期效果",
  "confidence": 0.85,
  "status": "resolved | monitoring | escalated"
}}

判断 status 原则：
- resolved：数据已恢复正常或已自动修复
- monitoring：问题已定位，需观察是否持续
- escalated：超出自动处理范围，需人工介入
"""


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


def _fetch_new_alerts(ch) -> list[dict]:
    """获取近10分钟内未处理的告警"""
    rows = ch.query("""
        SELECT alert_id, alert_time, alert_type, severity, table_name,
               field_name, detail, ai_suggestion, metric_value, threshold_value
        FROM stream.ai_quality_alerts
        WHERE alert_time >= now() - INTERVAL 10 MINUTE
        ORDER BY alert_time DESC
        LIMIT 20
    """).result_rows
    alerts = []
    for r in rows:
        alert_id = str(r[0]) if r[0] else str(uuid.uuid4())
        if alert_id not in _HANDLED:
            alerts.append({
                'alert_id':        alert_id,
                'alert_time':      r[1],
                'alert_type':      r[2],
                'severity':        r[3],
                'table_name':      r[4],
                'field_name':      r[5],
                'detail':          r[6],
                'ai_suggestion':   r[7],
                'metric_value':    float(r[8]) if r[8] else 0.0,
                'threshold_value': float(r[9]) if r[9] else 0.0,
            })
    return alerts


def _collect_context(ch, alert: dict) -> dict:
    """采集告警时段的多维上下文数据"""
    alert_time = alert['alert_time']
    ctx = {}

    # 最近5个分钟窗口
    try:
        rows = ch.query("""
            SELECT window_start, order_cnt, total_gmv, avg_price, top_category
            FROM dws.realtime_minute_stats
            WHERE window_start >= now() - INTERVAL 30 MINUTE
            ORDER BY window_start DESC LIMIT 10
        """).result_rows
        ctx['minute_stats'] = [
            {'t': str(r[0]), 'order_cnt': r[1], 'gmv': round(float(r[2]), 0),
             'avg_price': round(float(r[3]), 2), 'top_cat': r[4]}
            for r in rows
        ]
    except Exception:
        ctx['minute_stats'] = []

    # 近5分钟取消率
    try:
        r = ch.query("""
            SELECT countIf(order_status='canceled') AS c, count() AS t,
                   round(countIf(order_status='canceled') / count(), 4) AS rate
            FROM ods.orders_stream
            WHERE event_time >= now() - INTERVAL 5 MINUTE
        """).first_row
        ctx['cancel_rate_5min'] = {'canceled': r[0], 'total': r[1], 'rate': float(r[2])}
    except Exception:
        ctx['cancel_rate_5min'] = {}

    # 品类异常分布（近10分钟）
    try:
        rows = ch.query("""
            SELECT product_category, count() AS cnt, round(avg(price), 2) AS avg_p
            FROM ods.orders_stream
            WHERE event_time >= now() - INTERVAL 10 MINUTE
            GROUP BY product_category ORDER BY cnt DESC LIMIT 5
        """).result_rows
        ctx['top_categories'] = [{'cat': r[0], 'cnt': r[1], 'avg_price': r[2]} for r in rows]
    except Exception:
        ctx['top_categories'] = []

    # ETL 最近质量分
    try:
        r = ch.query("""
            SELECT round(avg(quality_score), 1), count()
            FROM stream.etl_audit_log
            WHERE run_time >= now() - INTERVAL 1 HOUR
        """).first_row
        ctx['etl_quality'] = {'avg_score': float(r[0] or 0), 'run_count': r[1]}
    except Exception:
        ctx['etl_quality'] = {}

    return ctx


def _collect_reconciliation(ch) -> dict:
    """获取最新的 Lambda 对账状态"""
    try:
        rows = ch.query("""
            SELECT check_date, batch_order_cnt, stream_order_cnt,
                   cnt_diff_pct, check_status
            FROM stream.lambda_reconciliation
            ORDER BY check_time DESC LIMIT 3
        """).result_rows
        return [{'date': str(r[0]), 'batch_cnt': r[1], 'stream_cnt': r[2],
                 'diff_pct': r[3], 'status': r[4]} for r in rows]
    except Exception:
        return []


@llm_retry
def _llm_investigate(alert: dict, context: dict, reconciliation: list) -> dict:
    from openai import OpenAI
    client = OpenAI(api_key=cfg.api_key, base_url=cfg.api_base_url, timeout=30.0)

    alert_info = json.dumps({
        'type': alert['alert_type'], 'severity': alert['severity'],
        'detail': alert['detail'], 'ai_suggestion': alert['ai_suggestion'],
        'metric_value': alert['metric_value'],
        'threshold_value': alert['threshold_value'],
        'time': str(alert['alert_time']),
    }, ensure_ascii=False, indent=2)

    context_str = json.dumps(context, ensure_ascii=False, indent=2)
    recon_str   = json.dumps(reconciliation, ensure_ascii=False, indent=2) if reconciliation else '暂无对账记录'

    resp = client.chat.completions.create(
        model=cfg.llm_model,
        messages=[{'role': 'user', 'content': _INVESTIGATION_PROMPT.format(
            alert_info=alert_info,
            context_data=context_str,
            reconciliation_info=recon_str,
        )}],
        temperature=0.2,
        max_tokens=600,
    )
    raw = resp.choices[0].message.content.strip()
    import re
    match = re.search(r'\{.*\}', raw, re.DOTALL)
    if not match:
        return {
            'root_cause': '无法解析LLM响应', 'impact_scope': '未知',
            'auto_action': '无', 'action_result': '解析失败',
            'confidence': 0.3, 'status': 'escalated',
        }
    return json.loads(match.group())


def _auto_execute(ch, alert: dict, investigation: dict) -> str:
    """执行安全的自动处置动作"""
    action = investigation.get('auto_action', '').lower()
    result = '无需操作'

    # 如果 ETL 质量分偏低，触发 ETL 扫描
    if any(kw in action for kw in ['etl', '重扫', '重跑', 'rescan']):
        try:
            # 触发 AI ETL Agent 单次运行（异步，不阻塞）
            import subprocess
            subprocess.Popen(
                [sys.executable, 'ai_etl/ai_etl_agent.py'],
                cwd=os.path.join(os.path.dirname(__file__), '..'),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            result = 'ETL Agent 已触发重新扫描'
            log.info('[自动操作] 触发 ETL Agent 重扫：%s', alert['alert_id'])
        except Exception as e:
            result = f'触发 ETL 失败：{e}'

    return result


def investigate_alert(ch, alert: dict) -> dict:
    """对单条告警执行完整排查流程"""
    log.info('[排查] [%s] %s', alert['severity'], alert['detail'][:80])

    context = _collect_context(ch, alert)
    recon   = _collect_reconciliation(ch)

    try:
        result = _llm_investigate(alert, context, recon)
    except Exception as e:
        log.error('LLM 排查失败：%s', e)
        result = {
            'root_cause': f'LLM分析失败：{e}', 'impact_scope': '未知',
            'auto_action': '无', 'action_result': '分析失败',
            'confidence': 0.0, 'status': 'escalated',
        }

    action_result = _auto_execute(ch, alert, result)
    result['action_result'] = action_result

    ch.insert(
        'stream.alert_investigations',
        [[
            str(uuid.uuid4()), alert['alert_id'], alert['alert_type'], alert['severity'],
            datetime.now(), result['root_cause'], result['impact_scope'],
            result.get('auto_action', '无'), action_result,
            float(result.get('confidence', 0.5)), result['status'],
            json.dumps({'alert': alert, 'context': context}, ensure_ascii=False, default=str),
        ]],
        column_names=['investigation_id', 'alert_id', 'alert_type', 'alert_severity',
                      'investigation_time', 'root_cause', 'impact_scope',
                      'auto_action', 'action_result', 'confidence', 'status', 'raw_context'],
    )

    _HANDLED.add(alert['alert_id'])
    log.info('[排查完成] status=%s confidence=%.2f root=%s',
             result['status'], result.get('confidence', 0), result['root_cause'][:60])
    return result


def run_once() -> int:
    ch = _get_ch()
    alerts = _fetch_new_alerts(ch)
    if not alerts:
        log.debug('无新告警')
        return 0
    log.info('发现 %d 条新告警，开始排查...', len(alerts))
    for alert in alerts:
        try:
            investigate_alert(ch, alert)
        except Exception as e:
            log.error('排查告警 %s 失败：%s', alert['alert_id'], e)
    return len(alerts)


def run_loop(interval: int = 60):
    log.info('告警排查服务启动，每 %ds 轮询一次', interval)
    while True:
        try:
            run_once()
        except Exception as e:
            log.error('排查循环异常：%s', e)
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description='AI 告警自动排查服务')
    parser.add_argument('--loop', type=int, default=60, help='循环间隔秒数（0=单次）')
    args = parser.parse_args()
    if args.loop > 0:
        run_loop(args.loop)
    else:
        n = run_once()
        print(f'处理 {n} 条告警')


if __name__ == '__main__':
    main()
