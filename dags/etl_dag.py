# time and airflow liabraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl_logic import extract_data, truncate_tables, load_data, send_success_email

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "edw_grp_fact_load",
    default_args = default_args,
    description = "Group Life Facts Pipeline DAG",
    schedule_interval = "@daily",
)

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    dag=dag,
)

truncate_task = PythonOperator(
    task_id="truncate_tables",
    python_callable=truncate_tables,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

email_task = PythonOperator(
    task_id="send_success_email",
    python_callable=send_success_email,
    dag=dag,
)

# Task dependencies: Extract → Truncate → Load → Email
extract_task >> truncate_task >> load_task >> email_task