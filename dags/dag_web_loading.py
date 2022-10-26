from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from functions import *
from loading import *

default_args = {
    'owner': 'Maico Bernal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

path_base = './datasets/base/'
path_precio = './datasets/prices'


with DAG('UpdateDatabases', schedule_interval='@once', default_args=default_args) as dag:
    StartPipeline = EmptyOperator(
        task_id = 'StartPipeline',
        dag = dag
        )

    PythonLoad = PythonOperator(
        task_id="LoadNewPrices",
        python_callable=LoadNewPrecios,
        )

    Send2SQL = PythonOperator(
        task_id="Send2SQL",
        python_callable=UploadNewPrecios,
        )

    CheckNewPricesQuery = PythonOperator(
        task_id="CheckNewPricesQuery",
        python_callable=MakeQueryUpdated,
        )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


StartPipeline >> PythonLoad >> Send2SQL >> CheckNewPricesQuery >> FinishPipeline