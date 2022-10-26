from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from functions import *
from etl import *

default_args = {
    'owner': 'Maico Bernal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

path_base = '/opt/airflow/dags/datasets/base'
path_precio = '/opt/airflow/dags/datasets/prices'

with DAG('UpdateDatabases', schedule_interval='@once', default_args=default_args) as dag:
    StartPipeline = EmptyOperator(
        task_id = 'StartPipeline',
        dag = dag
        )

    PythonLoadAndUpload = PythonOperator(
        task_id="LoadAndUploadNewPrices",
        python_callable=LoadAndUploadNewPrecios,
        )

    CheckNewPricesQuery = PythonOperator(
        task_id="CheckNewPricesQuery",
        python_callable=MakeQuery,
        )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


StartPipeline >> PythonLoadAndUpload >> CheckNewPricesQuery >> FinishPipeline