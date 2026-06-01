#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
特征计算管道 — Feature Pipeline
从 ClickHouse 实时数据计算特征，写入离线存储并同步到在线 Redis

运行模式：
  一次性：python feature_store/pipeline.py --group user_behavior
  持续刷新：python feature_store/pipeline.py --loop 300
"""
import os
import sys
import time
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('feature_pipeline')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=120,
    )


def compute_and_store(ch, group_name: str, feature_name: str,
                      computation_sql: str, feature_type: str = 'FLOAT64',
                      version: int = 1) -> int:
    """
    执行特征计算 SQL，结果写入 feature_store.feature_values
    使用 INSERT INTO ... SELECT 模式，避免 Python 中转，效率最高

    要求 computation_sql 返回列：entity_id, feature_value, feature_time
    """
    clean_sql = computation_sql.strip().rstrip(';')
    insert_sql = f"""
    INSERT INTO feature_store.feature_values
        (entity_id, group_name, feature_name, feature_value,
         feature_value_str, feature_time, computed_at, version)
    SELECT
        toString(entity_id)     AS entity_id,
        '{group_name}'          AS group_name,
        '{feature_name}'        AS feature_name,
        toFloat64OrZero(toString(feature_value)) AS feature_value,
        toString(feature_value) AS feature_value_str,
        feature_time,
        now()                   AS computed_at,
        {version}               AS version
    FROM ({clean_sql})
    """
    try:
        ch.command(insert_sql)
        # 查询写入行数
        cnt_rows = ch.query(f"""
            SELECT count() FROM feature_store.feature_values
            WHERE group_name = '{group_name}'
              AND feature_name = '{feature_name}'
              AND computed_at >= now() - INTERVAL 10 SECOND
        """).first_row
        count = int(cnt_rows[0]) if cnt_rows else 0
        log.info('[计算完成] %s.%s → %d 条', group_name, feature_name, count)
        return count
    except Exception as e:
        log.error('[计算失败] %s.%s：%s', group_name, feature_name, e)
        return 0


def compute_group(ch, group_name: str) -> dict:
    """计算一个特征组内所有特征，并同步到 Redis"""
    from feature_store.online_store import OnlineFeatureStore

    try:
        rows = ch.query(f"""
            SELECT feature_name, computation_sql, feature_type, online_ttl, version
            FROM feature_store.feature_definitions
            WHERE group_name = '{group_name}' AND is_active = 1
        """).result_rows
    except Exception as e:
        log.error('获取特征定义失败 group=%s：%s', group_name, e)
        return {}

    online = OnlineFeatureStore()
    stats = {'group': group_name, 'computed': 0, 'synced': 0, 'errors': 0}

    for feat_name, sql, feat_type, ttl, version in rows:
        if not sql:
            continue
        try:
            cnt = compute_and_store(ch, group_name, feat_name, sql, feat_type, int(version))
            stats['computed'] += cnt
            # 同步到 Redis
            synced = online.sync_from_offline(group_name, feat_name, limit=10000)
            stats['synced'] += synced
        except Exception as e:
            log.error('计算/同步 %s.%s 失败：%s', group_name, feat_name, e)
            stats['errors'] += 1

    log.info('[特征组] %s 计算完成：%d 条，同步 %d 条，错误 %d 个',
             group_name, stats['computed'], stats['synced'], stats['errors'])
    return stats


def compute_all_groups(ch) -> list[dict]:
    """计算所有注册的特征组"""
    try:
        rows = ch.query("""
            SELECT DISTINCT group_name FROM feature_store.feature_definitions
            WHERE is_active = 1
        """).result_rows
        groups = [r[0] for r in rows]
    except Exception as e:
        log.error('获取特征组列表失败：%s', e)
        return []

    log.info('开始计算所有特征组：%s', groups)
    results = []
    for group in groups:
        result = compute_group(ch, group)
        results.append(result)
    return results


def run_loop(interval: int = 300):
    """持续循环计算所有特征（默认每5分钟）"""
    log.info('特征计算管道启动，间隔 %ds', interval)
    ch = _get_ch()
    while True:
        t0 = time.time()
        try:
            results = compute_all_groups(ch)
            total = sum(r.get('computed', 0) for r in results)
            log.info('本轮计算完成：%d 个特征组，%d 条特征值', len(results), total)
        except Exception as e:
            log.error('计算循环异常：%s', e)
        elapsed = time.time() - t0
        sleep_time = max(0, interval - elapsed)
        time.sleep(sleep_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='特征计算管道')
    parser.add_argument('--group', default=None, help='指定计算的特征组（不传则计算所有）')
    parser.add_argument('--loop', type=int, default=0, help='循环间隔秒（0=单次）')
    args = parser.parse_args()

    ch = _get_ch()
    if args.loop > 0:
        run_loop(args.loop)
    elif args.group:
        result = compute_group(ch, args.group)
        print(f'完成：{result}')
    else:
        results = compute_all_groups(ch)
        for r in results:
            print(r)
