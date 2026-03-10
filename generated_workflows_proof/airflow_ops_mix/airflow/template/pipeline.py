from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(dag_id="airflow_ops_mix", start_date=datetime(2024,1,1), schedule=None, catchup=False) as dag:
    def _fn_ingest_api():
        print("task ingest_api")
    ingest_api = PythonOperator(task_id="ingest_api", python_callable=_fn_ingest_api)
    normalize_raw = BashOperator(task_id="normalize_raw", bash_command="echo normalize_raw")
    def _fn_load_warehouse():
        print("task load_warehouse")
    load_warehouse = PythonOperator(task_id="load_warehouse", python_callable=_fn_load_warehouse)

    ingest_api >> normalize_raw
    normalize_raw >> load_warehouse
