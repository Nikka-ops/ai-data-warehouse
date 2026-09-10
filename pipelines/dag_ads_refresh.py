# -*- coding: utf-8 -*-
"""
Airflow DAG：ADS 刷新

调度这条线上的任务和 Flink 常驻流作业是两套运行时，这是刻意的：

    常驻流作业   由 Flink 自己管生命周期 —— checkpoint 定期落盘，
                 失败按 exponential-delay 自动重启并从上次位点追赶积压，
                 不需要外部调度器介入，也不该被外部调度器停掉。
    本 DAG       周期性触发一次性任务，跑完就退出。失败了重试，
                 重试还失败就告警，不影响任何常驻进程。

把两者混在一个调度器里的典型后果是：调度器一重启，实时链路跟着断；
或者某个批任务把 Flink 的 slot 占满，流作业排不上队。分开就没这些事。

调度频率 10 分钟：ADS 是排行榜和跨天 BITMAP 去重，
看板对它的新鲜度要求本来就不是秒级 —— 秒级那部分走的是
ads.v_today_* 普通视图，直接查 Flink 刚写完的 DWS，不经过这里。
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# 项目根目录被整个挂进容器（见 docker-compose 的 airflow 服务），
# 这样 pipelines.* 和 common.* 才 import 得到
sys.path.insert(0, '/opt/airflow/project')

default_args = {
    'owner':            'ai-dw',
    'retries':          2,
    'retry_delay':      timedelta(minutes=1),
    'email_on_failure': False,
}

with DAG(
    dag_id='ads_refresh',
    description='刷新 BITMAP 日活与 ADS 排行表（与 Flink 常驻作业运行时分离）',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # 同时只允许一个实例：BITMAP 刷新是「先删当天再写」，
    # 两个实例并发会出现「A 删完、B 还没写完」的空窗
    max_active_runs=1,
    default_args=default_args,
    tags=['ads', 'batch', 'bitmap'],
) as dag:

    start = EmptyOperator(task_id='start')

    def task_refresh_today(**_):
        from pipelines.refresh_ads import refresh_one_day
        from common.doris_client import get_client
        from datetime import date

        doris = get_client()
        stats = refresh_one_day(doris, date.today())
        print(f'ADS 刷新完成：{stats}')
        return stats

    refresh_today = PythonOperator(
        task_id='refresh_today',
        python_callable=task_refresh_today,
    )

    def task_refresh_yesterday(**_):
        """
        补刷昨天。

        跨零点那几分钟里，Flink 可能还在往昨天的窗口补写迟到数据，
        当时刷出来的昨日排行是不完整的。每轮顺手补一次，
        代价是一次小查询，换来的是历史数据最终收敛到正确值。
        """
        from pipelines.refresh_ads import refresh_one_day
        from common.doris_client import get_client
        from datetime import date, timedelta as td

        doris = get_client()
        stats = refresh_one_day(doris, date.today() - td(days=1))
        print(f'昨日补刷完成：{stats}')
        return stats

    refresh_yesterday = PythonOperator(
        task_id='refresh_yesterday',
        python_callable=task_refresh_yesterday,
    )

    end = EmptyOperator(task_id='end')

    # 串行而不是并行：两个任务都会写 dws.user_active_daily，
    # 并发写同一张 Aggregate 表没有必要冒这个险
    start >> refresh_today >> refresh_yesterday >> end
