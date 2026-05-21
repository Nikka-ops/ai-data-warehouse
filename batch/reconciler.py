#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lambda 架构 — 批实时数据一致性校验器
对比离线层（dws.batch_daily_stats）和实时层（ods.orders_stream）
在同一天的订单量/GMV，写入 stream.lambda_reconciliation。

运行：
  python batch/reconciler.py              # 校验昨日
  python batch/reconciler.py --days 7    # 校验最近7天
  python batch/reconciler.py --loop 600  # 每10分钟循环校验
"""
import os, sys, time, argparse
from datetime import date, timedelta, datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('reconciler')

CONSISTENT_THRESHOLD = 0.02   # 差异 < 2% 视为一致


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=60,
    )


def reconcile_date(ch, target_date: date) -> dict:
    """对账单日数据：批处理层 vs 实时层"""
    # 离线层：dws.batch_daily_stats
    batch_row = ch.query(f"""
        SELECT sum(order_cnt), round(sum(total_gmv), 2)
        FROM dws.batch_daily_stats
        WHERE stat_date = '{target_date}'
    """).first_row
    batch_cnt, batch_gmv = batch_row[0] or 0, batch_row[1] or 0.0

    # 实时层：ods.orders_stream（Kafka 落地表）
    stream_row = ch.query(f"""
        SELECT count(DISTINCT order_id), round(sum(price), 2)
        FROM ods.orders_stream
        WHERE event_date = '{target_date}'
    """).first_row
    stream_cnt, stream_gmv = stream_row[0] or 0, stream_row[1] or 0.0

    # 计算差异
    ref_cnt = max(batch_cnt, stream_cnt, 1)
    ref_gmv = max(batch_gmv, stream_gmv, 1.0)
    cnt_diff_pct = abs(batch_cnt - stream_cnt) / ref_cnt
    gmv_diff_pct = abs(batch_gmv - stream_gmv) / ref_gmv

    is_consistent = int(cnt_diff_pct < CONSISTENT_THRESHOLD
                        and gmv_diff_pct < CONSISTENT_THRESHOLD)

    if batch_cnt == 0 and stream_cnt == 0:
        status = 'OK'    # 该日无数据（正常）
    elif batch_cnt == 0:
        status = 'WARN'  # 批处理层缺失数据
    elif stream_cnt == 0:
        status = 'OK'    # 历史数据仅在批处理层，实时层无历史（正常）
    elif is_consistent:
        status = 'OK'
    elif max(cnt_diff_pct, gmv_diff_pct) > 0.10:
        status = 'MISMATCH'
    else:
        status = 'WARN'

    result = {
        'check_date':       target_date,
        'batch_order_cnt':  batch_cnt,
        'stream_order_cnt': stream_cnt,
        'batch_gmv':        batch_gmv,
        'stream_gmv':       stream_gmv,
        'cnt_diff_pct':     round(cnt_diff_pct * 100, 2),
        'gmv_diff_pct':     round(gmv_diff_pct * 100, 2),
        'is_consistent':    is_consistent,
        'check_status':     status,
    }
    log.info('[%s] batch=%d stream=%d cnt_diff=%.1f%% status=%s',
             target_date, batch_cnt, stream_cnt, cnt_diff_pct * 100, status)
    return result


def run_reconciliation(days: int = 1):
    """对账最近 N 天"""
    ch = _get_ch()
    today = date.today()
    results = []

    for delta in range(1, days + 1):
        target = today - timedelta(days=delta)
        result = reconcile_date(ch, target)
        results.append(result)

        ch.insert(
            'stream.lambda_reconciliation',
            [[
                datetime.now(), result['check_date'],
                result['batch_order_cnt'], result['stream_order_cnt'],
                result['batch_gmv'], result['stream_gmv'],
                result['cnt_diff_pct'], result['gmv_diff_pct'],
                result['is_consistent'], result['check_status'],
            ]],
            column_names=['check_time', 'check_date', 'batch_order_cnt', 'stream_order_cnt',
                          'batch_gmv', 'stream_gmv', 'cnt_diff_pct', 'gmv_diff_pct',
                          'is_consistent', 'check_status'],
        )

    mismatches = [r for r in results if r['check_status'] == 'MISMATCH']
    if mismatches:
        log.warning('发现 %d 天数据不一致！', len(mismatches))
    else:
        log.info('对账完成，所有日期数据一致')

    return results


def run_loop(interval: int = 600, days: int = 1):
    log.info('对账服务启动，每 %ds 对账最近 %d 天', interval, days)
    while True:
        try:
            run_reconciliation(days)
        except Exception as e:
            log.error('对账失败：%s', e)
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description='Lambda 批实时对账')
    parser.add_argument('--days', type=int, default=1, help='对账天数')
    parser.add_argument('--loop', type=int, default=0, help='循环间隔秒数（0=单次）')
    args = parser.parse_args()

    if args.loop > 0:
        run_loop(interval=args.loop, days=args.days)
    else:
        results = run_reconciliation(days=args.days)
        for r in results:
            print(f"[{r['check_date']}] batch={r['batch_order_cnt']} "
                  f"stream={r['stream_order_cnt']} "
                  f"diff={r['cnt_diff_pct']}% status={r['check_status']}")


if __name__ == '__main__':
    main()
