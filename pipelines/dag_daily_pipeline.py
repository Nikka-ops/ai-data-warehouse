"""
Airflow DAG：AI 数仓每日全流程调度
流程：下载数据 → ODS → DWD → DWS/ADS → 数据验证
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys

# 把 pipelines 目录加入 Python 路径
sys.path.insert(0, '/opt/airflow/dags')

default_args = {
    'owner':            'ai-dw',
    'retries':          2,
    'retry_delay':      timedelta(minutes=3),
    'email_on_failure': False,
}

with DAG(
    dag_id='ai_warehouse_daily_pipeline',
    description='AI 数仓每日全流程：ODS → DWD → DWS → ADS',
    schedule_interval='0 2 * * *',      # 每天凌晨 2 点
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['ai-dw', 'etl', 'clickhouse'],
) as dag:

    # ── 开始节点 ─────────────────────────────────────────────
    start = EmptyOperator(task_id='start')

    # ── Task 1：加载 ODS 层 ──────────────────────────────────
    def task_load_ods():
        from etl_ods import run_ods_load
        run_ods_load()

    load_ods = PythonOperator(
        task_id='load_ods',
        python_callable=task_load_ods,
    )

    # ── Task 2：加工 DWD 层 ──────────────────────────────────
    def task_load_dwd():
        from etl_dwd import run_dwd_load
        run_dwd_load()

    load_dwd = PythonOperator(
        task_id='load_dwd',
        python_callable=task_load_dwd,
    )

    # ── Task 3：加工 DWS / ADS 层 ────────────────────────────
    def task_load_dws_ads():
        from etl_dws_ads import run_dws_ads_load
        run_dws_ads_load()

    load_dws_ads = PythonOperator(
        task_id='load_dws_ads',
        python_callable=task_load_dws_ads,
    )

    # ── Task 4：数据质量验证 ─────────────────────────────────
    def task_data_quality_check(**context):
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
            username=os.getenv('CLICKHOUSE_USER', 'admin'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'admin123'),
        )

        checks = {
            'ods.orders_raw':      50_000,   # 至少 5 万行
            'dwd.order_detail':    100_000,  # 至少 10 万行
            'dws.order_daily':     400,      # 至少 400 天
            'ads.monthly_kpi':     20,       # 至少 20 个月
        }

        failed = []
        for table, min_rows in checks.items():
            cnt = client.query(f'SELECT count() FROM {table}').first_row[0]
            status = "✅" if cnt >= min_rows else "❌"
            print(f"  {status} {table}: {cnt:,} 行（阈值 {min_rows:,}）")
            if cnt < min_rows:
                failed.append(table)

        if failed:
            raise ValueError(f"数据质量检查失败：{failed}")

        print("✅ 所有数据质量检查通过")

    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=task_data_quality_check,
    )

    # ── 结束节点 ─────────────────────────────────────────────
    end = EmptyOperator(task_id='end')

    # ── 依赖关系 ─────────────────────────────────────────────
    start >> load_ods >> load_dwd >> load_dws_ads >> quality_check >> end
