# -*- coding: utf-8 -*-
"""
Flink 指标采集器

之前建 stream.job_metrics 表时只有看板在读它，一直没有写入侧 ——
这个脚本补上：定时从 Flink REST API 拉取作业指标，落库供看板画趋势、
供 SQL 查询「作业健不健康」。它和巡检 Agent 是两回事：
    巡检 Agent  管「现在有没有异常、要不要处置」（实时判断 + 研判）
    采集器      管「把指标存下来能回溯」（时序留档）

运行：
    python -m ops_agent.collect --once        # 采集一次
    python -m ops_agent.collect --interval 60  # 每 60 秒采集一次
"""

from __future__ import annotations

import time
import argparse
from datetime import datetime

from ops_agent import probes


def collect_once() -> int:
    """采集一轮 Flink 指标写入 stream.job_metrics，返回写入行数"""
    flink = probes.probe_flink()
    if flink['status'] == 'UNREACHABLE':
        print(f'[采集] Flink 不可达，跳过：{flink["detail"]}')
        return 0

    jobs = flink.get('jobs', [])
    if not jobs:
        print('[采集] 无运行中的作业')
        return 0

    try:
        from common.doris_client import get_client
        doris = get_client()
    except Exception as e:
        print(f'[采集] Doris 客户端不可用：{e}')
        return 0

    now = datetime.now()
    rows = []
    for job in jobs:
        rows.append([
            now.date(),
            now,
            job.get('job_name', '') or job.get('job_id', ''),
            job.get('job_id', ''),
            job.get('state', ''),
            int((job.get('uptime_ms', 0) or 0) / 1000),
            0,                                          # parallelism：REST 需额外查，暂留 0
            0.0, 0.0,                                   # in/out per sec
            0,                                          # kafka_consumer_lag：由 kafka 探针单独采
            0.0,                                        # busy_time
            'OK',                                       # backpressure_level
            0,                                          # watermark_lag
            int(job.get('last_ckpt_duration_ms', 0) or 0),
            int(job.get('last_ckpt_size_bytes', 0) or 0),
            int(job.get('ckpt_failed', 0) or 0),
            0,                                          # restart_count
        ])

    try:
        doris.insert(
            'stream.job_metrics', rows,
            column_names=[
                'stat_date', 'collect_time', 'job_name', 'job_id', 'job_state',
                'uptime_seconds', 'parallelism', 'records_in_per_sec',
                'records_out_per_sec', 'kafka_consumer_lag', 'busy_time_ms_per_sec',
                'backpressure_level', 'current_watermark_lag', 'last_ckpt_duration_ms',
                'last_ckpt_size_bytes', 'ckpt_failed_count', 'restart_count',
            ])
        print(f'[采集] {now:%H:%M:%S} 写入 {len(rows)} 个作业指标')
        return len(rows)
    except Exception as e:
        print(f'[采集] 落库失败：{e}')
        return 0


def run(interval: int) -> None:
    print(f'Flink 指标采集器启动，每 {interval} 秒一次')
    while True:
        try:
            collect_once()
        except KeyboardInterrupt:
            print('\n停止采集')
            break
        except Exception as e:
            print(f'[采集] 本轮失败：{e}')
        time.sleep(interval)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Flink 指标采集器')
    ap.add_argument('--once', action='store_true', help='只采集一次')
    ap.add_argument('--interval', type=int, default=60, help='采集间隔秒数')
    args = ap.parse_args()
    if args.once:
        collect_once()
    else:
        run(args.interval)
