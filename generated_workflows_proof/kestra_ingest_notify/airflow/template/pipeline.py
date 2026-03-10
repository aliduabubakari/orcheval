from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(dag_id="kestra_ingest_notify", start_date=datetime(2024,1,1), schedule=None, catchup=False) as dag:
    def _fn_poll_events():
        print("task poll_events")
    poll_events = PythonOperator(task_id="poll_events", python_callable=_fn_poll_events)
    def _fn_stage_payload():
        print("task stage_payload")
    stage_payload = PythonOperator(task_id="stage_payload", python_callable=_fn_stage_payload)
    def _fn_merge_events():
        print("task merge_events")
    merge_events = PythonOperator(task_id="merge_events", python_callable=_fn_merge_events)
    def _fn_notify_ops():
        print("task notify_ops")
    notify_ops = PythonOperator(task_id="notify_ops", python_callable=_fn_notify_ops)

    merge_events >> notify_ops
    poll_events >> stage_payload
    stage_payload >> merge_events
