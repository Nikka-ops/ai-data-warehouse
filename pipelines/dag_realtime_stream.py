# -*- coding: utf-8 -*-
"""
Airflow DAG：实时流处理调度
每分钟触发一次流处理窗口
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys, os

sys.path.insert(0, '/opt/airflow/dags')

default_args = {
    'owner':            'ai-dw',
    'retries':          1,
    'retry_delay':      timedelta(seconds=30),
    'email_on_failure': False,
}

with DAG(
    dag_id='realtime_stream_processor',
    description='每分钟处理一次实时数据窗口：聚合 → AI质检 → 告警 → DWD更新',
    schedule_interval='* * * * *',   # 每分钟执行
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,               # 同时只允许1个实例运行，防止并发冲突
    default_args=default_args,
    tags=['realtime', 'kafka', 'stream'],
) as dag:

    start = EmptyOperator(task_id='start')

    def task_process_window(**context):
        """处理当前分钟窗口"""
        # 动态导入避免模块缓存问题
        import importlib
        import sys

        # 确保从最新代码加载
        if 'stream_processor' in sys.modules:
            del sys.modules['stream_processor']

        from kafka.stream_processor import process_window
        process_window()

    process = PythonOperator(
        task_id='process_minute_window',
        python_callable=task_process_window,
        execution_timeout=timedelta(seconds=50),  # 50秒超时，留10秒余量
    )

    def task_check_consumer_lag(**context):
        """检查 Kafka 消费延迟"""
        import clickhouse_connect
        import os

        ch = clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
            username=os.getenv('CLICKHOUSE_USER', 'admin'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'admin123'),
        )

        # 检查最近1分钟是否有数据入库
        recent = ch.query(
            "SELECT count() FROM ods.orders_stream "
            "WHERE _ingest_time >= now() - INTERVAL 2 MINUTE"
        ).first_row[0]

        if recent == 0:
            print("⚠️ 警告：最近2分钟无新数据入库，请检查 Kafka 生产者和 ClickHouse 消费状态")
        else:
            print(f"✅ 消费正常：最近2分钟入库 {recent} 条")

    check_lag = PythonOperator(
        task_id='check_consumer_lag',
        python_callable=task_check_consumer_lag,
    )

    end = EmptyOperator(task_id='end')

    start >> process >> check_lag >> end
