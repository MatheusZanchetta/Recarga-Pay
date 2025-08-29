import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from airflow.operators.python import ShortCircuitOperator

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from rp_case.cdi_ingest.job import  cdi_ingest_job
from rp_case.raw_to_curated.job import  raw_to_curated_job
from rp_case.curated_to_analytics.job import  analytics_job

# Configurações padrão da DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "end_date": datetime(2024,10,31),
    #"retries": 1,
    #"retry_delay": timedelta(minutes=1),
}

# Criação da DAG
with DAG(
    dag_id="rp-case",
    default_args=default_args,
    description="Pipeline - Recarga Pay",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["cdi_ingest", "rp"],
) as dag:
    dag.doc_md = __doc__

    def data_flow():
        cdi_ingest = PythonOperator(
            task_id="cdi_ingest",
            python_callable=cdi_ingest_job,
            op_kwargs={'execution_date': '{{ ds }}'},
        )
        raw_to_curated = PythonOperator(
            task_id="raw_to_curated",
            python_callable=raw_to_curated_job,
            op_kwargs={'execution_date': '{{ ds }}'},
        )
        analytics_task = PythonOperator(
            task_id="analytics_data",
            python_callable=analytics_job,
            op_kwargs={"execution_date": "{{ ds }}"},
        )
        cdi_ingest>> raw_to_curated >> analytics_task

    data_flow()
