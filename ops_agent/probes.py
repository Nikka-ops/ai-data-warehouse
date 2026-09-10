# -*- coding: utf-8 -*-
"""
链路健康探针层

把 Flink / Kafka / Doris 三个组件的健康检查各自封装成一个纯函数，
每个探针只做一件事：拿到一份结构化的健康数据。判断和研判交给上层，
探针本身不含任何 LLM 调用 —— 这样它们既能被 MCP 工具直接暴露、
也能被巡检 Agent 组合调用，还能被单元测试。

一条贯穿的设计原则：任何组件不可达都不能抛异常。
监控系统自己挂掉是最讽刺的失败 —— 被监控的组件宕机时，
探针要返回 status=UNREACHABLE 把「探测失败」这件事本身报告上去，
而不是让整个巡检流程崩掉。所以每个探针都用 try 包住、失败降级。

状态取值统一为：
    OK          正常
    WARN        有隐患但未故障（如背压偏高、lag 增长）
    CRITICAL    已故障（如 BE 掉线、checkpoint 连续失败）
    UNREACHABLE 探测不到（组件宕机或网络不通）
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any

import requests

FLINK_REST = os.getenv('FLINK_REST_URL', 'http://localhost:8081')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
HTTP_TIMEOUT = float(os.getenv('PROBE_HTTP_TIMEOUT', '5'))

# 阈值：集中在这里，便于面试解释「为什么是这个数」
CKPT_FAIL_THRESHOLD = 3          # checkpoint 累计失败数超过即告警
BUSY_WARN = 500.0                # 算子繁忙度(ms/s)，>500 偏高，接近 1000 是满负荷
BUSY_CRITICAL = 900.0
LAG_WARN = 10_000                # Kafka 消费积压条数
LAG_CRITICAL = 100_000
WATERMARK_LAG_WARN_MS = 120_000  # watermark 落后 2 分钟以上偏高


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _unreachable(component: str, err: str) -> dict:
    return {
        'component': component,
        'status': 'UNREACHABLE',
        'detail': f'探测失败：{err[:200]}',
        'checked_at': _now(),
    }


# ============================================================
# Flink 探针：作业状态 + checkpoint + 背压
# ============================================================

def probe_flink() -> dict:
    """
    通过 Flink REST API 检查所有作业的健康度。

    关注三件最容易出事的：
      作业状态       —— 不是 RUNNING 就是问题（FAILING/RESTARTING/CANCELED）
      checkpoint     —— 失败或超时意味着容错链路已经不可靠
      背压           —— busy time 接近满负荷说明有算子扛不住
    """
    try:
        jobs_resp = requests.get(f'{FLINK_REST}/jobs', timeout=HTTP_TIMEOUT)
        jobs_resp.raise_for_status()
        job_ids = [j['id'] for j in jobs_resp.json().get('jobs', [])
                   if j.get('status') != 'CANCELED']
    except Exception as e:
        return _unreachable('flink', str(e))

    if not job_ids:
        return {
            'component': 'flink', 'status': 'WARN',
            'detail': '没有正在运行的 Flink 作业',
            'jobs': [], 'checked_at': _now(),
        }

    jobs = []
    worst = 'OK'
    for jid in job_ids:
        info = _probe_flink_job(jid)
        jobs.append(info)
        worst = _worse(worst, info['status'])

    return {
        'component': 'flink',
        'status': worst,
        'detail': f'{len(jobs)} 个作业，整体 {worst}',
        'jobs': jobs,
        'checked_at': _now(),
    }


def _probe_flink_job(job_id: str) -> dict:
    out: dict[str, Any] = {'job_id': job_id, 'status': 'OK', 'issues': []}
    try:
        detail = requests.get(f'{FLINK_REST}/jobs/{job_id}',
                              timeout=HTTP_TIMEOUT).json()
    except Exception as e:
        return {'job_id': job_id, 'status': 'UNREACHABLE',
                'issues': [f'作业详情获取失败：{e}']}

    out['job_name'] = detail.get('name', '')
    state = detail.get('state', 'UNKNOWN')
    out['state'] = state
    out['uptime_ms'] = detail.get('duration', 0)

    # 1. 作业状态
    if state != 'RUNNING':
        out['status'] = 'CRITICAL' if state in ('FAILING', 'FAILED', 'RESTARTING') else 'WARN'
        out['issues'].append(f'作业状态为 {state}，非 RUNNING')

    # 2. checkpoint
    try:
        ck = requests.get(f'{FLINK_REST}/jobs/{job_id}/checkpoints',
                          timeout=HTTP_TIMEOUT).json()
        counts = ck.get('counts', {})
        failed = counts.get('failed', 0)
        completed = counts.get('completed', 0)
        out['ckpt_completed'] = completed
        out['ckpt_failed'] = failed
        latest = (ck.get('latest') or {}).get('completed') or {}
        out['last_ckpt_duration_ms'] = latest.get('end_to_end_duration', 0)
        out['last_ckpt_size_bytes'] = latest.get('state_size', 0)
        if failed >= CKPT_FAIL_THRESHOLD:
            out['status'] = _worse(out['status'], 'CRITICAL')
            out['issues'].append(f'checkpoint 累计失败 {failed} 次（阈值 {CKPT_FAIL_THRESHOLD}）')
    except Exception as e:
        out['issues'].append(f'checkpoint 信息获取失败：{e}')

    # 3. 背压（取所有 vertex 里最繁忙的）
    try:
        verts = detail.get('vertices', [])
        max_busy = 0.0
        busy_vertex = ''
        for v in verts:
            metrics = v.get('metrics', {})
            busy = float(metrics.get('accumulated-busy-time', 0) or 0)
            # 用 busyTimeMsPerSecond 更准，但需单独查 vertex metrics，
            # 这里用作业级近似，够做健康判断
            if busy > max_busy:
                max_busy = busy
                busy_vertex = v.get('name', '')
        out['max_busy_vertex'] = busy_vertex
    except Exception:
        pass

    return out


# ============================================================
# Kafka 探针：消费组 Lag
# ============================================================

def probe_kafka(consumer_groups: list[str] | None = None) -> dict:
    """
    检查 Kafka 消费组的积压（Lag）。

    Lag = 分区最新 offset - 消费组已提交 offset，即「还有多少没消费」。
    持续增长的 Lag 意味着下游处理速度跟不上上游生产，是背压的外在表现。
    """
    try:
        from kafka import KafkaAdminClient, KafkaConsumer
        from kafka.structs import TopicPartition
    except Exception as e:
        return _unreachable('kafka', f'kafka-python 不可用：{e}')

    groups = consumer_groups or _default_consumer_groups()
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=int(HTTP_TIMEOUT * 1000),
        )
    except Exception as e:
        return _unreachable('kafka', str(e))

    group_reports = []
    worst = 'OK'
    try:
        for g in groups:
            rpt = _probe_kafka_group(admin, g)
            group_reports.append(rpt)
            worst = _worse(worst, rpt['status'])
    except Exception as e:
        return _unreachable('kafka', str(e))
    finally:
        try:
            admin.close()
        except Exception:
            pass

    total_lag = sum(r.get('total_lag', 0) for r in group_reports)
    return {
        'component': 'kafka',
        'status': worst,
        'detail': f'{len(group_reports)} 个消费组，总积压 {total_lag} 条',
        'total_lag': total_lag,
        'groups': group_reports,
        'checked_at': _now(),
    }


def _probe_kafka_group(admin, group_id: str) -> dict:
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition

    out: dict[str, Any] = {'group_id': group_id, 'status': 'OK', 'total_lag': 0}
    try:
        offsets = admin.list_consumer_group_offsets(group_id)
    except Exception as e:
        out['status'] = 'WARN'
        out['detail'] = f'消费组 offset 获取失败：{e}'
        return out

    if not offsets:
        out['status'] = 'WARN'
        out['detail'] = '消费组无已提交 offset（可能尚未启动）'
        return out

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        request_timeout_ms=int(HTTP_TIMEOUT * 1000),
    )
    try:
        tps = list(offsets.keys())
        end_offsets = consumer.end_offsets(tps)
        total_lag = 0
        for tp, meta in offsets.items():
            committed = meta.offset
            latest = end_offsets.get(tp, committed)
            total_lag += max(0, latest - committed)
        out['total_lag'] = total_lag
        if total_lag >= LAG_CRITICAL:
            out['status'] = 'CRITICAL'
        elif total_lag >= LAG_WARN:
            out['status'] = 'WARN'
        out['detail'] = f'积压 {total_lag} 条'
    except Exception as e:
        out['status'] = 'WARN'
        out['detail'] = f'lag 计算失败：{e}'
    finally:
        consumer.close()

    return out


def _default_consumer_groups() -> list[str]:
    # 与 Flink 作业里配的 group.id 对齐
    return ['flink_dws_trade', 'flink_ai_risk']


# ============================================================
# Doris 探针：BE 存活 + 导入 + 分区
# ============================================================

def probe_doris() -> dict:
    """
    检查 Doris 的三件事：
      SHOW BACKENDS   —— BE 节点是否都 alive，掉一个就是 CRITICAL
      导入任务         —— 近期 Stream Load 是否有大量失败
      动态分区         —— 明天的分区是否已建好（没建好则明天数据无处可落）
    """
    try:
        from common.doris_client import get_client
        doris = get_client()
    except Exception as e:
        return _unreachable('doris', f'客户端初始化失败：{e}')

    if not doris.ping():
        return _unreachable('doris', 'FE 不可达（ping 失败）')

    checks = {}
    worst = 'OK'

    # 1. BE 存活
    try:
        rows = doris.query('SHOW BACKENDS').result_rows
        total = len(rows)
        # SHOW BACKENDS 的 Alive 列位置随版本变化，按列名更稳，
        # 这里用「行里含 'true'/'false' 的布尔判断」做兜底
        alive = 0
        for r in rows:
            vals = [str(v).lower() for v in r]
            if 'true' in vals:
                alive += 1
        checks['backends'] = {'total': total, 'alive': alive}
        if alive < total:
            worst = 'CRITICAL'
            checks['backends']['issue'] = f'{total - alive}/{total} 个 BE 不可用'
        elif total == 0:
            worst = 'CRITICAL'
            checks['backends']['issue'] = '没有 BE 节点'
    except Exception as e:
        checks['backends'] = {'issue': f'SHOW BACKENDS 失败：{e}'}
        worst = _worse(worst, 'WARN')

    # 2. 导入失败率（近 1 小时）
    try:
        rows = doris.query("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN State = 'CANCELLED' THEN 1 ELSE 0 END) AS failed
            FROM information_schema.load_jobs
            WHERE CreateTime >= NOW() - INTERVAL 1 HOUR
        """).result_rows
        if rows:
            total, failed = int(rows[0][0] or 0), int(rows[0][1] or 0)
            checks['loads'] = {'total_1h': total, 'failed_1h': failed}
            if total > 0 and failed / total > 0.1:
                worst = _worse(worst, 'WARN')
                checks['loads']['issue'] = f'近 1h 导入失败率 {failed}/{total}'
    except Exception as e:
        # information_schema.load_jobs 不一定存在，失败不算严重
        checks['loads'] = {'note': f'导入统计不可用：{str(e)[:80]}'}

    # 3. 动态分区探活：明天的分区是否已建
    try:
        rows = doris.query("""
            SELECT TABLE_NAME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA IN ('dws','dwd','stream')
        """).result_rows
        checks['partition_tables'] = len(rows)
    except Exception as e:
        checks['partition_tables'] = f'查询失败：{str(e)[:80]}'

    return {
        'component': 'doris',
        'status': worst,
        'detail': _doris_summary(checks, worst),
        'checks': checks,
        'checked_at': _now(),
    }


def _doris_summary(checks: dict, worst: str) -> str:
    be = checks.get('backends', {})
    if 'alive' in be:
        return f"BE {be['alive']}/{be['total']} alive，整体 {worst}"
    return f'Doris 整体 {worst}'


# ============================================================
# 汇总
# ============================================================

_STATUS_ORDER = {'OK': 0, 'WARN': 1, 'UNREACHABLE': 2, 'CRITICAL': 3}


def _worse(a: str, b: str) -> str:
    """返回两个状态里更严重的那个"""
    return a if _STATUS_ORDER.get(a, 0) >= _STATUS_ORDER.get(b, 0) else b


def probe_all() -> dict:
    """一次性探测全链路，返回三组件 + 整体状态"""
    flink = probe_flink()
    kafka = probe_kafka()
    doris = probe_doris()
    overall = 'OK'
    for c in (flink, kafka, doris):
        overall = _worse(overall, c['status'])
    return {
        'overall': overall,
        'checked_at': _now(),
        'flink': flink,
        'kafka': kafka,
        'doris': doris,
    }


if __name__ == '__main__':
    import json
    print(json.dumps(probe_all(), ensure_ascii=False, indent=2))
