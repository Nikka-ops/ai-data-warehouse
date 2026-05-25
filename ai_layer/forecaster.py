# -*- coding: utf-8 -*-
"""
实时预测分析器
每分钟读取 dws.realtime_minute_stats，用 Holt 双指数平滑预测未来10分钟，
结果写入 dws.realtime_forecast。
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from utils.retry import ch_retry

log = get_logger('forecaster')

METRICS   = ['order_cnt', 'total_gmv', 'avg_price']
HORIZON   = 10   # 预测未来10分钟
LOOKBACK  = 60   # 使用最近60分钟做基线拟合
ALPHA     = 0.35  # 水平平滑系数
BETA      = 0.10  # 趋势平滑系数


@ch_retry
def _get_ch():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port,
        username=cfg.ch_user, password=cfg.ch_password,
        connect_timeout=10, send_receive_timeout=30,
    )


# ── Holt 双指数平滑 ───────────────────────────────────────────

def holt_forecast(values: list[float], horizon: int = HORIZON,
                  alpha: float = ALPHA, beta: float = BETA) -> tuple[list, list, list]:
    """
    Holt 双指数平滑（考虑线性趋势）。
    返回：(预测值列表, 95%置信下界, 95%置信上界)
    """
    n = len(values)
    if n < 4:
        last = values[-1] if values else 0.0
        return [last] * horizon, [last * 0.8] * horizon, [last * 1.2] * horizon

    # 初始化
    level = float(values[0])
    trend = float(values[1] - values[0])

    residuals = []
    for v in values[1:]:
        pred      = level + trend
        residuals.append(abs(float(v) - pred))
        new_level = alpha * float(v) + (1 - alpha) * (level + trend)
        trend     = beta  * (new_level - level) + (1 - beta) * trend
        level     = new_level

    # 预测
    forecasts = [max(0.0, level + (i + 1) * trend) for i in range(horizon)]

    # 残差标准差用于置信区间（95% → 1.96σ）
    n_res = len(residuals)
    std   = (sum(r ** 2 for r in residuals) / n_res) ** 0.5 if n_res else 0.0
    ci    = 1.96 * std

    lower = [max(0.0, f - ci * (1 + i * 0.1)) for i, f in enumerate(forecasts)]
    upper = [f + ci * (1 + i * 0.1) for i, f in enumerate(forecasts)]

    return forecasts, lower, upper


# ── 主流程 ────────────────────────────────────────────────────

def run_once() -> dict:
    """执行一次预测，返回写入行数"""
    ch = _get_ch()

    # 读取最近 LOOKBACK 分钟的统计数据
    rows = ch.query(f"""
        SELECT window_start, order_cnt, total_gmv, avg_price
        FROM dws.realtime_minute_stats
        WHERE window_start >= now() - INTERVAL {LOOKBACK} MINUTE
        ORDER BY window_start ASC
    """).result_rows

    if len(rows) < 4:
        log.info('数据不足（%d 行），跳过预测', len(rows))
        return {'rows_written': 0, 'reason': 'insufficient_data'}

    # 按指标分别预测
    now_minute = datetime.now().replace(second=0, microsecond=0)
    insert_rows = []

    metric_values = {
        'order_cnt': [float(r[1]) for r in rows],
        'total_gmv': [float(r[2]) for r in rows],
        'avg_price': [float(r[3]) for r in rows],
    }

    for metric, values in metric_values.items():
        preds, lowers, uppers = holt_forecast(values)
        for h, (pred, lo, hi) in enumerate(zip(preds, lowers, uppers), start=1):
            forecast_time = now_minute + timedelta(minutes=h)
            insert_rows.append([
                forecast_time, metric,
                round(pred, 2), round(lo, 2), round(hi, 2),
                h, 'holt_double', datetime.now()
            ])

    ch.insert(
        'dws.realtime_forecast',
        insert_rows,
        column_names=['forecast_time', 'metric', 'predicted', 'lower_bound',
                      'upper_bound', 'horizon', 'model', '_created_at'],
    )
    log.info('预测完成：%d 行写入 dws.realtime_forecast（3指标×%d步）',
             len(insert_rows), HORIZON)
    return {'rows_written': len(insert_rows)}


def run_loop(interval: int = 60):
    log.info('预测器启动，每 %ds 更新一次', interval)
    while True:
        try:
            result = run_once()
            log.debug('本轮结果：%s', result)
        except Exception as e:
            log.error('预测异常：%s', e)
        time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='实时预测分析器')
    parser.add_argument('--loop', type=int, default=60, help='循环间隔秒数（0=单次）')
    args = parser.parse_args()
    if args.loop > 0:
        run_loop(args.loop)
    else:
        print(run_once())
