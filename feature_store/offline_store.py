#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
离线特征存储 — Offline Feature Store
负责特征的批量计算、历史存储与 Point-in-Time 正确查询。

核心职责：
  1. 从 feature_definitions 中读取 computation_sql，执行后写入 feature_values
  2. 支持按特征组批量计算（compute_group）
  3. 提供 PIT 正确的历史特征拉取（get_historical_features）
  4. 定时调度特征刷新循环（run_scheduled_refresh）
"""
import os
import sys
import time
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('offline_store')


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=300,
    )


class OfflineFeatureStore:
    """
    离线特征存储：
    - 执行特征计算 SQL，将结果写入 feature_store.feature_values
    - 支持历史特征查询（Point-in-Time Correct）
    - 提供定时刷新调度
    """

    def __init__(self):
        self._ch = None

    def _conn(self):
        """获取或重连 ClickHouse 客户端"""
        if self._ch is None:
            self._ch = _get_ch()
        return self._ch

    # ──────────────────────────────────────────────────────────────────────────
    # 特征计算
    # ──────────────────────────────────────────────────────────────────────────

    def compute_feature(
        self,
        group_name: str,
        feature_name: str,
        batch_size: int = 10_000,
    ) -> int:
        """
        执行指定特征的 computation_sql，将结果写入 feature_values。

        computation_sql 必须返回三列：
            entity_id (String), feature_value (Float64), feature_time (DateTime)
        或四列（含字符串值）：
            entity_id, feature_value, feature_value_str, feature_time

        Args:
            group_name:   特征组名称
            feature_name: 特征名称
            batch_size:   单批次写入行数限制

        Returns:
            int: 写入的行数（-1 表示失败）
        """
        ch = self._conn()

        # 查询 computation_sql
        try:
            rows = ch.query(
                """
                SELECT computation_sql, online_ttl, version
                FROM feature_store.feature_definitions
                WHERE group_name = {g:String}
                  AND feature_name = {f:String}
                  AND is_active = 1
                LIMIT 1
                """,
                parameters={'g': group_name, 'f': feature_name},
            ).result_rows
        except Exception as e:
            log.error('查询 computation_sql 失败（%s.%s）：%s', group_name, feature_name, e)
            return -1

        if not rows:
            log.warning('特征定义不存在或已下线：%s.%s', group_name, feature_name)
            return -1

        sql, online_ttl, version = rows[0]
        if not sql or not sql.strip():
            log.warning('特征 %s.%s 的 computation_sql 为空，跳过', group_name, feature_name)
            return 0

        # 使用 INSERT INTO ... SELECT 模式，让 ClickHouse 在服务端完成数据流转
        # computation_sql 需要返回: entity_id, feature_value[, feature_value_str], feature_time
        insert_sql = f"""
        INSERT INTO feature_store.feature_values
            (entity_id, group_name, feature_name, feature_value, feature_value_str,
             feature_time, computed_at, version)
        SELECT
            entity_id,
            '{group_name}'      AS group_name,
            '{feature_name}'    AS feature_name,
            toFloat64(feature_value)         AS feature_value,
            assumeNotNull(toString(ifNull(feature_value_str, '')))  AS feature_value_str,
            feature_time,
            now()               AS computed_at,
            {int(version)}      AS version
        FROM (
            {sql}
        )
        LIMIT {batch_size}
        """

        try:
            ch.command(insert_sql)
            # 估算写入行数（ClickHouse command 不直接返回影响行数）
            count_row = ch.query(
                """
                SELECT count()
                FROM feature_store.feature_values
                WHERE group_name = {g:String}
                  AND feature_name = {f:String}
                  AND computed_at >= now() - INTERVAL 60 SECOND
                """,
                parameters={'g': group_name, 'f': feature_name},
            ).result_rows
            written = count_row[0][0] if count_row else 0
            log.info('特征 %s.%s 计算完成，写入约 %d 行', group_name, feature_name, written)
            return written
        except Exception as e:
            log.error('执行特征计算失败（%s.%s）：%s', group_name, feature_name, e, exc_info=True)
            return -1

    def compute_group(self, group_name: str) -> dict:
        """
        批量计算特征组内所有活跃特征。

        Args:
            group_name: 特征组名称

        Returns:
            dict: {feature_name: rows_written}，失败的特征值为 -1
        """
        ch = self._conn()
        try:
            rows = ch.query(
                """
                SELECT feature_name
                FROM feature_store.feature_definitions
                WHERE group_name = {g:String} AND is_active = 1
                ORDER BY feature_name
                """,
                parameters={'g': group_name},
            ).result_rows
        except Exception as e:
            log.error('查询特征组 %s 成员失败：%s', group_name, e)
            return {}

        feature_names = [r[0] for r in rows]
        if not feature_names:
            log.warning('特征组 %s 下无活跃特征', group_name)
            return {}

        results = {}
        for fname in feature_names:
            log.debug('计算特征：%s.%s', group_name, fname)
            results[fname] = self.compute_feature(group_name, fname)

        succeeded = sum(1 for v in results.values() if v >= 0)
        failed = len(results) - succeeded
        log.info(
            '特征组 %s 计算完成：%d 成功，%d 失败',
            group_name, succeeded, failed,
        )
        return results

    # ──────────────────────────────────────────────────────────────────────────
    # 历史特征拉取（Point-in-Time Correct）
    # ──────────────────────────────────────────────────────────────────────────

    def get_historical_features(
        self,
        entity_ids: list[str],
        group_name: str,
        feature_names: list[str],
        as_of_time: Optional[datetime] = None,
    ):
        """
        拉取历史特征值，支持 Point-in-Time 正确语义。

        对每个 entity_id，返回截止 as_of_time 的最新特征值。
        若 as_of_time 为 None，则返回当前最新值。

        Args:
            entity_ids:    实体 ID 列表
            group_name:    特征组名称
            feature_names: 需要拉取的特征名称列表
            as_of_time:    查询时间点；None 表示当前时刻

        Returns:
            pandas.DataFrame: 列为 [entity_id] + feature_names，行为各实体特征值
        """
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError('pandas 未安装，请执行 pip install pandas')

        if not entity_ids or not feature_names:
            return pd.DataFrame(columns=['entity_id'] + list(feature_names))

        as_of_ts = as_of_time or datetime.now()
        as_of_str = as_of_ts.strftime('%Y-%m-%d %H:%M:%S')

        # 构建实体列表字面量（ClickHouse Array 语法）
        entity_list = ', '.join(f"'{eid}'" for eid in entity_ids)
        feature_list = ', '.join(f"'{fn}'" for fn in feature_names)

        # 使用子查询 + argMax 实现 PIT 正确查询
        # argMax(feature_value, feature_time) 取截止 as_of_time 的最新值
        pivot_cases = '\n        '.join(
            f"argMaxIf(feature_value, feature_time, feature_name = '{fn}') AS `{fn}`"
            for fn in feature_names
        )

        pit_sql = f"""
        SELECT
            entity_id,
            {pivot_cases}
        FROM feature_store.feature_values
        WHERE group_name = '{group_name}'
          AND feature_name IN ({feature_list})
          AND entity_id IN ({entity_list})
          AND feature_time <= '{as_of_str}'
        GROUP BY entity_id
        ORDER BY entity_id
        """

        try:
            result = self._conn().query(pit_sql)
            cols = ['entity_id'] + list(feature_names)
            df = pd.DataFrame(result.result_rows, columns=cols)

            # 补全缺失的 entity_id 行（NaN 填充）
            all_entities = pd.DataFrame({'entity_id': entity_ids})
            df = all_entities.merge(df, on='entity_id', how='left')

            log.info(
                'PIT 查询完成：group=%s，特征数=%d，实体数=%d，as_of=%s',
                group_name, len(feature_names), len(entity_ids), as_of_str,
            )
            return df

        except Exception as e:
            log.error('历史特征查询失败（%s）：%s', group_name, e, exc_info=True)
            return pd.DataFrame(columns=['entity_id'] + list(feature_names))

    def get_feature_stats(
        self,
        group_name: str,
        feature_name: str,
        lookback_hours: int = 24,
    ) -> dict:
        """
        获取特征在指定时间窗口内的统计信息。

        Args:
            group_name:     特征组名称
            feature_name:   特征名称
            lookback_hours: 回溯时间窗口（小时）

        Returns:
            dict: {count, mean, std, min, p50, p95, max, null_rate}
        """
        try:
            rows = self._conn().query(
                """
                SELECT
                    count()                             AS cnt,
                    avg(feature_value)                  AS mean,
                    stddevPop(feature_value)            AS std,
                    min(feature_value)                  AS min_val,
                    quantile(0.5)(feature_value)        AS p50,
                    quantile(0.95)(feature_value)       AS p95,
                    max(feature_value)                  AS max_val,
                    countIf(feature_value = 0) / count() AS null_rate
                FROM feature_store.feature_values
                WHERE group_name = {g:String}
                  AND feature_name = {f:String}
                  AND feature_time >= now() - INTERVAL {h:UInt32} HOUR
                """,
                parameters={'g': group_name, 'f': feature_name, 'h': lookback_hours},
            ).result_rows

            if rows:
                r = rows[0]
                return {
                    'count': r[0], 'mean': r[1], 'std': r[2],
                    'min': r[3], 'p50': r[4], 'p95': r[5], 'max': r[6],
                    'null_rate': r[7],
                }
        except Exception as e:
            log.error('获取特征统计失败（%s.%s）：%s', group_name, feature_name, e)
        return {}

    # ──────────────────────────────────────────────────────────────────────────
    # 定时调度刷新
    # ──────────────────────────────────────────────────────────────────────────

    def run_scheduled_refresh(self, interval: int = 300):
        """
        持续调度所有活跃特征的刷新。

        每隔 interval 秒查询所有活跃特征，并逐一调用 compute_feature。
        设计为长驻进程，生产环境建议配合 supervisor/systemd 管理。

        Args:
            interval: 刷新间隔（秒），默认 300 秒（5 分钟）
        """
        log.info('离线特征刷新调度器启动，刷新间隔 %d 秒', interval)
        while True:
            start = time.time()
            try:
                rows = self._conn().query(
                    """
                    SELECT DISTINCT group_name, feature_name
                    FROM feature_store.feature_definitions
                    WHERE is_active = 1
                    ORDER BY group_name, feature_name
                    """
                ).result_rows

                total = len(rows)
                success = 0
                for group, feat in rows:
                    try:
                        result = self.compute_feature(group, feat)
                        if result >= 0:
                            success += 1
                    except Exception as e:
                        log.warning('特征刷新异常（%s.%s）：%s', group, feat, e)

                elapsed = time.time() - start
                log.info(
                    '本轮刷新完成：%d/%d 特征成功，耗时 %.1f 秒',
                    success, total, elapsed,
                )

            except Exception as e:
                log.error('调度循环异常：%s', e, exc_info=True)

            # 精确等待剩余时间
            elapsed = time.time() - start
            sleep_time = max(0.0, interval - elapsed)
            if sleep_time > 0:
                log.debug('等待下一轮刷新，剩余 %.1f 秒', sleep_time)
                time.sleep(sleep_time)


# ── 全局单例 ────────────────────────────────────────────────────────────────
_offline_store: Optional[OfflineFeatureStore] = None


def get_offline_store() -> OfflineFeatureStore:
    """获取全局 OfflineFeatureStore 单例"""
    global _offline_store
    if _offline_store is None:
        _offline_store = OfflineFeatureStore()
    return _offline_store


if __name__ == '__main__':
    store = OfflineFeatureStore()
    # 示例：刷新所有特征组
    try:
        groups = store._conn().query(
            "SELECT DISTINCT group_name FROM feature_store.feature_definitions WHERE is_active = 1"
        ).result_rows
        for (g,) in groups:
            result = store.compute_group(g)
            print(f'特征组 {g} 刷新结果：{result}')
    except Exception as exc:
        print(f'刷新失败：{exc}')
