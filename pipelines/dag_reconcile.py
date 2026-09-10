# -*- coding: utf-8 -*-
"""
Airflow DAG：业务库 ↔ 数仓 对账

每小时跑一次，把 MySQL 和 Doris 的同口径数字比一遍，结果落
ads.reconcile_result。这是实时链路的验收环节 ——
Exactly-Once 和主键幂等是设计上的保证，对账是对这个保证的持续检验。

注意 strict=False：对账发现差异不让 DAG 失败。

原因是当天正在进行的时段本来就会有正常差异 —— 业务库已经写入的订单，
Flink 的窗口可能还没关闭（watermark 要等 5 分钟乱序容忍），
数仓侧的数字天然会略小一点。把这种时间差判成任务失败，
告警很快就会因为天天响而被忽略，那才是真正的隐患。

差异被完整记录在结果表里，看板和 Agent 读得到；
需要卡失败的场景（比如 T+1 的日终对账）单独用 --strict 跑一次即可。
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
    'retries':          1,
    'retry_delay':      timedelta(minutes=2),
    'email_on_failure': False,
}

with DAG(
    dag_id='dw_reconcile',
    description='业务库与数仓对账，差异落 ads.reconcile_result',
    schedule_interval='0 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['quality', 'reconcile'],
) as dag:

    start = EmptyOperator(task_id='start')

    def task_reconcile_today(**_):
        from pipelines.reconcile import run_checks
        from datetime import date

        results = run_checks(date.today())
        bad = [r for r in results if r['status'] == 'FAIL']
        for r in results:
            print(f"{r['status']:<5} {r['check_item']:<14} "
                  f"业务库={r['source_value']} 数仓={r['target_value']} "
                  f"差异={r['diff_value']} ({r['diff_pct']}%)")
        # 只打印不抛异常，理由见模块开头
        if bad:
            print(f'注意：{len(bad)} 项差异超过阈值，详见 ads.reconcile_result')
        return {'checked': len(results), 'failed': len(bad)}

    reconcile_today = PythonOperator(
        task_id='reconcile_today',
        python_callable=task_reconcile_today,
    )

    end = EmptyOperator(task_id='end')

    start >> reconcile_today >> end
