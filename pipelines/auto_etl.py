# -*- coding: utf-8 -*-
"""
自动 ETL 调度器
基于 APScheduler，无需 Airflow 即可运行完整 ETL 流水线。

调度逻辑（巴西时区，可通过 .env 配置）：
  01:00  ODS 层加载（CSV → ClickHouse）
  02:00  DWD 层加工（ODS 关联清洗）
  03:00  DWS/ADS 层聚合

运行方式：
  python pipelines/auto_etl.py          # 持续调度
  python pipelines/auto_etl.py --once   # 立即执行一次全量 ETL
  python pipelines/auto_etl.py --stage ods|dwd|ads  # 手动触发单层
"""
import os, sys, argparse
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import cfg
from utils.logger import get_logger
from pipelines.etl_ods     import run_ods_load
from pipelines.etl_dwd     import run_dwd_load
from pipelines.etl_dws_ads import run_dws_ads_load

log = get_logger('auto_etl')

# ── 各阶段任务（带错误捕获）────────────────────────────────────

def _run(name: str, fn):
    log.info('>>> 开始执行 %s', name)
    start = datetime.now()
    try:
        fn()
        elapsed = (datetime.now() - start).total_seconds()
        log.info('<<< %s 完成（耗时 %.1f 秒）', name, elapsed)
    except Exception as e:
        log.error('!!! %s 执行失败：%s', name, e, exc_info=True)
        raise


def task_ods():
    _run('ODS 层加载', run_ods_load)


def task_dwd():
    _run('DWD 层加工', run_dwd_load)


def task_dws_ads():
    _run('DWS/ADS 层聚合', run_dws_ads_load)


def task_full_pipeline():
    """全量 ETL：ODS → DWD → DWS/ADS"""
    log.info('===== 全量 ETL 流水线启动 =====')
    task_ods()
    task_dwd()
    task_dws_ads()
    log.info('===== 全量 ETL 流水线完成 =====')


# ── 调度事件监听 ──────────────────────────────────────────────

def _on_event(event):
    if event.exception:
        log.error('任务 [%s] 执行异常：%s', event.job_id, event.exception)
    else:
        log.info('任务 [%s] 执行成功', event.job_id)


# ── 主调度器 ──────────────────────────────────────────────────

def _parse_cron(cron_str: str) -> dict:
    """将 '0 1 * * *' 格式转为 APScheduler cron kwargs"""
    parts = cron_str.split()
    if len(parts) != 5:
        raise ValueError(f'无效 cron 表达式: {cron_str}')
    minute, hour, day, month, day_of_week = parts
    return dict(minute=minute, hour=hour, day=day, month=month, day_of_week=day_of_week)


def run_scheduler():
    scheduler = BlockingScheduler(timezone=cfg.etl_timezone)
    scheduler.add_listener(_on_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    scheduler.add_job(
        task_ods, 'cron', id='etl_ods',
        **_parse_cron(cfg.etl_ods_cron),
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        task_dwd, 'cron', id='etl_dwd',
        **_parse_cron(cfg.etl_dwd_cron),
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        task_dws_ads, 'cron', id='etl_dws_ads',
        **_parse_cron(cfg.etl_ads_cron),
        misfire_grace_time=3600,
    )

    log.info('自动 ETL 调度器已启动（时区：%s）', cfg.etl_timezone)
    log.info('  ODS 调度：%s', cfg.etl_ods_cron)
    log.info('  DWD 调度：%s', cfg.etl_dwd_cron)
    log.info('  ADS 调度：%s', cfg.etl_ads_cron)
    log.info('按 Ctrl+C 停止')

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info('调度器已停止')


# ── CLI 入口 ──────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='AI 数仓自动 ETL 调度器')
    parser.add_argument('--once',  action='store_true', help='立即执行一次全量 ETL 后退出')
    parser.add_argument('--stage', choices=['ods', 'dwd', 'ads'], help='手动执行指定层')
    args = parser.parse_args()

    if args.once:
        task_full_pipeline()
    elif args.stage == 'ods':
        task_ods()
    elif args.stage == 'dwd':
        task_dwd()
    elif args.stage == 'ads':
        task_dws_ads()
    else:
        run_scheduler()
