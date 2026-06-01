#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
特征漂移监控 — Feature Drift Monitor
定期检测特征分布变化，及早发现数据质量问题与模型退化风险。

核心指标：
  • PSI（Population Stability Index）：群体稳定性指数，量化分布偏移程度
      PSI < 0.1   → 稳定（Stable），无需干预
      0.1 ≤ PSI < 0.25 → 监控（Monitor），关注趋势
      PSI ≥ 0.25  → 漂移（Drift），触发告警

  • null_rate：空值/零值率，检测上游数据缺失问题
  • mean/std/p50/p95：分布统计指标，辅助判断中心偏移和尾部变化

设计思路：
  1. compute_stats：对指定特征的最近 N 小时数据计算分布统计
  2. compute_psi：与基线分布比较，计算 PSI 分数
  3. check_all_features：遍历所有活跃特征，生成完整漂移报告
  4. write_drift_stats：持久化统计结果到 feature_store.drift_stats
  5. run_drift_check_loop：定时运行完整监控流程
"""
import os
import sys
import math
import time
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('drift_monitor')

# PSI 阈值常量
PSI_STABLE = 0.1       # < 0.1：稳定
PSI_MONITOR = 0.25     # 0.1~0.25：需关注；>= 0.25：漂移告警

# 默认 PSI 分桶数（10 等宽箱）
PSI_BUCKETS = 10

# 基线窗口：以前 7 天为基线，与最近 24h 比较
_BASELINE_HOURS = 7 * 24


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=120,
    )


class DriftMonitor:
    """
    特征漂移监控：计算分布统计、PSI，并持久化结果。
    """

    def __init__(self):
        self._ch = None

    def _conn(self):
        if self._ch is None:
            self._ch = _get_ch()
        return self._ch

    # ──────────────────────────────────────────────────────────────────────────
    # 分布统计
    # ──────────────────────────────────────────────────────────────────────────

    def compute_stats(
        self,
        group_name: str,
        feature_name: str,
        window_hours: int = 24,
    ) -> dict:
        """
        计算特征在最近 window_hours 小时内的分布统计。

        统计指标：
          - count：样本数
          - mean：均值
          - std：标准差
          - min/max：极值
          - p50/p95：中位数和 95 分位数
          - null_rate：零值率（特征值为 0 且 str 为空视为空）
          - value_distribution：10 个等宽分桶的占比（用于 PSI 计算）

        Args:
            group_name:   特征组名称
            feature_name: 特征名称
            window_hours: 统计时间窗口（小时）

        Returns:
            dict: 包含所有统计指标的字典
        """
        stats: dict = {
            'group_name': group_name,
            'feature_name': feature_name,
            'window_hours': window_hours,
            'check_time': datetime.now().isoformat(),
            'count': 0, 'mean': 0.0, 'std': 0.0,
            'min': 0.0, 'max': 0.0, 'p50': 0.0, 'p95': 0.0,
            'null_rate': 0.0,
            'value_distribution': [],
        }

        try:
            rows = self._conn().query(
                f"""
                SELECT
                    count()                              AS cnt,
                    avg(feature_value)                   AS mean,
                    stddevPop(feature_value)             AS std,
                    min(feature_value)                   AS min_val,
                    max(feature_value)                   AS max_val,
                    quantile(0.5)(feature_value)         AS p50,
                    quantile(0.95)(feature_value)        AS p95,
                    countIf(feature_value = 0 AND feature_value_str = '')
                        / count()                        AS null_rate
                FROM feature_store.feature_values
                WHERE group_name   = '{group_name}'
                  AND feature_name = '{feature_name}'
                  AND feature_time >= now() - INTERVAL {window_hours} HOUR
                """
            ).result_rows

            if not rows or rows[0][0] == 0:
                log.warning(
                    '特征 %s.%s 在最近 %dh 内无数据',
                    group_name, feature_name, window_hours,
                )
                return stats

            r = rows[0]
            min_val = float(r[3]) if r[3] is not None else 0.0
            max_val = float(r[4]) if r[4] is not None else 0.0
            stats.update({
                'count': int(r[0]),
                'mean': float(r[1]) if r[1] is not None else 0.0,
                'std': float(r[2]) if r[2] is not None else 0.0,
                'min': min_val,
                'max': max_val,
                'p50': float(r[5]) if r[5] is not None else 0.0,
                'p95': float(r[6]) if r[6] is not None else 0.0,
                'null_rate': float(r[7]) if r[7] is not None else 0.0,
            })

            # 计算分桶分布（用于 PSI）
            stats['value_distribution'] = self._compute_distribution(
                group_name, feature_name, window_hours, min_val, max_val,
            )

        except Exception as e:
            log.error('计算特征统计失败（%s.%s）：%s', group_name, feature_name, e)

        return stats

    def _compute_distribution(
        self,
        group_name: str,
        feature_name: str,
        window_hours: int,
        min_val: float,
        max_val: float,
        n_buckets: int = PSI_BUCKETS,
    ) -> list:
        """
        将特征值分成 n_buckets 个等宽桶，返回每桶的占比列表。

        Returns:
            list[float]: 长度为 n_buckets，各元素为该桶占比（总和约为 1）
        """
        if min_val == max_val:
            # 所有值相同，全部落入第一桶
            dist = [0.0] * n_buckets
            dist[0] = 1.0
            return dist

        try:
            rows = self._conn().query(
                f"""
                SELECT
                    widthBucket(feature_value, {min_val}, {max_val}, {n_buckets}) AS bucket,
                    count() AS cnt
                FROM feature_store.feature_values
                WHERE group_name   = '{group_name}'
                  AND feature_name = '{feature_name}'
                  AND feature_time >= now() - INTERVAL {window_hours} HOUR
                GROUP BY bucket
                ORDER BY bucket
                """
            ).result_rows

            total = sum(r[1] for r in rows)
            if total == 0:
                return [0.0] * n_buckets

            # widthBucket 返回 1~n_buckets（超界为 0 或 n_buckets+1）
            bucket_counts: dict = {int(r[0]): int(r[1]) for r in rows}
            dist = [bucket_counts.get(i, 0) / total for i in range(1, n_buckets + 1)]
            return dist

        except Exception as e:
            log.debug('计算分桶分布失败（%s.%s）：%s', group_name, feature_name, e)
            # 均匀分布作为兜底
            return [1.0 / n_buckets] * n_buckets

    # ──────────────────────────────────────────────────────────────────────────
    # PSI 计算
    # ──────────────────────────────────────────────────────────────────────────

    def compute_psi(
        self,
        current_stats: dict,
        baseline_stats: dict,
        epsilon: float = 1e-6,
    ) -> float:
        """
        计算群体稳定性指数（Population Stability Index，PSI）。

        公式：PSI = Σ (P_current - P_baseline) × ln(P_current / P_baseline)

        Args:
            current_stats:  当前窗口的分布统计（含 value_distribution）
            baseline_stats: 基线分布统计（含 value_distribution）
            epsilon:        平滑系数，防止零桶导致数值不稳定（ln(0) 未定义）

        Returns:
            float: PSI 分数
              < 0.1  → 稳定
              0.1~0.25 → 需监控
              ≥ 0.25 → 漂移
        """
        current_dist: list = current_stats.get('value_distribution', [])
        baseline_dist: list = baseline_stats.get('value_distribution', [])

        if not current_dist or not baseline_dist:
            log.warning('分布数据缺失，PSI 返回 0.0')
            return 0.0

        n = min(len(current_dist), len(baseline_dist))
        if n == 0:
            return 0.0

        psi = 0.0
        for p_curr, p_base in zip(current_dist[:n], baseline_dist[:n]):
            # epsilon 平滑，避免除零和 log(0)
            p_curr = max(float(p_curr), epsilon)
            p_base = max(float(p_base), epsilon)
            psi += (p_curr - p_base) * math.log(p_curr / p_base)

        return round(psi, 6)

    def interpret_psi(self, psi: float) -> str:
        """将 PSI 数值转换为人类可读的状态标签"""
        if psi < PSI_STABLE:
            return 'stable'
        elif psi < PSI_MONITOR:
            return 'monitor'
        else:
            return 'drift'

    # ──────────────────────────────────────────────────────────────────────────
    # 全量特征漂移检测
    # ──────────────────────────────────────────────────────────────────────────

    def check_all_features(self, window_hours: int = 24) -> list:
        """
        遍历所有活跃特征，计算当前分布统计并与基线比较，生成漂移报告。

        Args:
            window_hours: 当前统计窗口时长（小时）

        Returns:
            list[dict]: 每个特征的漂移报告，包含：
              group_name, feature_name, mean_value, std_value, p50, p95,
              null_rate, psi_score, drift_status, drift_detected, check_time
        """
        try:
            rows = self._conn().query(
                """
                SELECT DISTINCT group_name, feature_name
                FROM feature_store.feature_definitions
                WHERE is_active = 1
                ORDER BY group_name, feature_name
                """
            ).result_rows
        except Exception as e:
            log.error('查询活跃特征列表失败：%s', e)
            return []

        if not rows:
            log.warning('没有活跃特征，跳过漂移检测')
            return []

        results = []
        for group_name, feature_name in rows:
            try:
                report = self._check_single_feature(group_name, feature_name, window_hours)
                results.append(report)
            except Exception as e:
                log.warning('特征 %s.%s 漂移检测失败：%s', group_name, feature_name, e)

        drift_count = sum(1 for r in results if r.get('drift_detected', 0))
        log.info(
            '漂移检测完成：%d 个特征，%d 个漂移告警',
            len(results), drift_count,
        )
        return results

    def _check_single_feature(
        self,
        group_name: str,
        feature_name: str,
        window_hours: int,
    ) -> dict:
        """对单个特征执行漂移检测，返回完整报告字典。"""
        # 当前窗口统计（默认 24h）
        current = self.compute_stats(group_name, feature_name, window_hours)
        # 基线统计（前 7 天）
        baseline = self.compute_stats(group_name, feature_name, _BASELINE_HOURS)

        psi = self.compute_psi(current, baseline)
        drift_status = self.interpret_psi(psi)

        # 综合漂移判断：PSI 超阈值 或 null_rate > 30%
        drift_detected = psi >= PSI_MONITOR or current.get('null_rate', 0.0) > 0.3

        report = {
            'group_name': group_name,
            'feature_name': feature_name,
            'check_time': datetime.now(),
            'mean_value': current.get('mean', 0.0),
            'std_value': current.get('std', 0.0),
            'p50': current.get('p50', 0.0),
            'p95': current.get('p95', 0.0),
            'null_rate': current.get('null_rate', 0.0),
            'psi_score': psi,
            'drift_status': drift_status,
            'drift_detected': 1 if drift_detected else 0,
            'sample_count': current.get('count', 0),
        }

        if drift_detected:
            log.warning(
                '[DRIFT] %s.%s  PSI=%.4f (%s)  null_rate=%.1f%%  n=%d',
                group_name, feature_name, psi, drift_status.upper(),
                current.get('null_rate', 0.0) * 100,
                current.get('count', 0),
            )
        else:
            log.debug(
                '[OK] %s.%s  PSI=%.4f  null_rate=%.1f%%',
                group_name, feature_name, psi,
                current.get('null_rate', 0.0) * 100,
            )

        return report

    # ──────────────────────────────────────────────────────────────────────────
    # 持久化
    # ──────────────────────────────────────────────────────────────────────────

    def write_drift_stats(self, stats_list: list) -> int:
        """
        将漂移统计结果批量写入 feature_store.drift_stats 表。

        Args:
            stats_list: check_all_features() 或 _check_single_feature() 的返回值列表

        Returns:
            int: 成功写入的行数
        """
        if not stats_list:
            return 0

        rows = []
        for s in stats_list:
            rows.append([
                s.get('feature_name', ''),
                s.get('group_name', ''),
                s.get('check_time', datetime.now()),
                float(s.get('mean_value', 0.0)),
                float(s.get('std_value', 0.0)),
                float(s.get('p50', 0.0)),
                float(s.get('p95', 0.0)),
                float(s.get('null_rate', 0.0)),
                float(s.get('psi_score', 0.0)),
                int(s.get('drift_detected', 0)),
            ])

        try:
            self._conn().insert(
                'feature_store.drift_stats',
                rows,
                column_names=[
                    'feature_name', 'group_name', 'check_time',
                    'mean_value', 'std_value', 'p50', 'p95', 'null_rate',
                    'psi_score', 'drift_detected',
                ],
            )
            log.info('漂移统计写入 ClickHouse：%d 条', len(rows))
            return len(rows)
        except Exception as e:
            log.error('写入 drift_stats 失败：%s', e, exc_info=True)
            return 0

    def get_drift_history(
        self,
        group_name: str,
        feature_name: str,
        lookback_hours: int = 168,
    ) -> list:
        """
        查询指定特征的历史漂移记录。

        Args:
            group_name:     特征组名称
            feature_name:   特征名称
            lookback_hours: 回溯时间（小时），默认 7 天（168h）

        Returns:
            list[dict]: 漂移历史，按时间降序排列
        """
        try:
            rows = self._conn().query(
                """
                SELECT check_time, mean_value, std_value, p50, p95,
                       null_rate, psi_score, drift_detected
                FROM feature_store.drift_stats
                WHERE group_name   = {g:String}
                  AND feature_name = {f:String}
                  AND check_time >= now() - INTERVAL {h:UInt32} HOUR
                ORDER BY check_time DESC
                """,
                parameters={'g': group_name, 'f': feature_name, 'h': lookback_hours},
            ).result_rows

            return [
                {
                    'check_time': r[0].isoformat() if hasattr(r[0], 'isoformat') else str(r[0]),
                    'mean': r[1], 'std': r[2], 'p50': r[3], 'p95': r[4],
                    'null_rate': r[5], 'psi_score': r[6], 'drift_detected': r[7],
                }
                for r in rows
            ]
        except Exception as e:
            log.error('查询漂移历史失败（%s.%s）：%s', group_name, feature_name, e)
            return []

    # ──────────────────────────────────────────────────────────────────────────
    # 定时监控循环
    # ──────────────────────────────────────────────────────────────────────────

    def run_drift_check_loop(
        self,
        interval: int = 3600,
        window_hours: int = 24,
    ):
        """
        持续运行漂移监控循环（每 interval 秒检测一次）。

        生产环境中建议通过 supervisor/systemd 管理此进程，
        或配合 APScheduler / Airflow DAG 进行精细调度。

        Args:
            interval:     检测间隔（秒），默认 3600（1小时）
            window_hours: 当前分布统计窗口（小时），默认 24
        """
        log.info(
            '特征漂移监控循环启动：检测间隔=%ds，统计窗口=%dh',
            interval, window_hours,
        )

        while True:
            loop_start = time.time()
            log.info(
                '开始特征漂移检测：%s',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            )

            try:
                stats_list = self.check_all_features(window_hours=window_hours)

                if stats_list:
                    written = self.write_drift_stats(stats_list)
                    drift_alerts = [s for s in stats_list if s.get('drift_detected')]
                    log.info(
                        '本轮完成：%d 特征，%d 告警，写入 %d 条统计',
                        len(stats_list), len(drift_alerts), written,
                    )
                    for alert in drift_alerts:
                        log.warning(
                            '[DRIFT ALERT] %s.%s  PSI=%.4f  null_rate=%.1f%%',
                            alert['group_name'], alert['feature_name'],
                            alert['psi_score'], alert['null_rate'] * 100,
                        )
                else:
                    log.info('本轮无活跃特征需要检测')

            except Exception as e:
                log.error('漂移监控循环异常：%s', e, exc_info=True)

            elapsed = time.time() - loop_start
            sleep_time = max(0.0, interval - elapsed)
            log.debug('本轮耗时 %.1fs，等待 %.1fs', elapsed, sleep_time)
            if sleep_time > 0:
                time.sleep(sleep_time)


# ── 全局单例 ────────────────────────────────────────────────────────────────
_drift_monitor: Optional[DriftMonitor] = None


def get_drift_monitor() -> DriftMonitor:
    """获取全局 DriftMonitor 单例"""
    global _drift_monitor
    if _drift_monitor is None:
        _drift_monitor = DriftMonitor()
    return _drift_monitor


# ── 便捷函数 ─────────────────────────────────────────────────────────────────

def compute_psi(current_stats: dict, baseline_stats: dict) -> float:
    """
    模块级便捷函数：计算 PSI 分数。

    Args:
        current_stats:  当前分布统计（来自 compute_stats）
        baseline_stats: 基线分布统计

    Returns:
        float: PSI 分数（< 0.1 稳定，0.1~0.25 监控，>= 0.25 漂移）
    """
    return get_drift_monitor().compute_psi(current_stats, baseline_stats)


if __name__ == '__main__':
    monitor = DriftMonitor()

    # 检测所有活跃特征，打印报告
    reports = monitor.check_all_features(window_hours=24)
    print(f'\n漂移检测报告（共 {len(reports)} 个特征）：')
    print(f'{"特征组":<20} {"特征名":<25} {"PSI":>8} {"状态":<10} {"null率":>8}')
    print('-' * 75)
    for r in sorted(reports, key=lambda x: x.get('psi_score', 0), reverse=True):
        status = 'DRIFT' if r.get('drift_detected') else 'OK'
        print(
            f"{r['group_name']:<20} {r['feature_name']:<25} "
            f"{r.get('psi_score', 0):>8.4f} {status:<10} "
            f"{r.get('null_rate', 0) * 100:>7.1f}%"
        )

    if reports:
        monitor.write_drift_stats(reports)
        print('\n统计已写入 feature_store.drift_stats')
