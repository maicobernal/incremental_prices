from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from functions import *
from loading import *
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

path_base = './datasets/base/'
path_precio = './datasets/prices'


with DAG('InitialLoading', schedule_interval='@once', default_args=default_args) as dag:
    StartPipeline = EmptyOperator(
        task_id = 'StartPipeline',
        dag = dag
        )

    PythonLoad1 = PythonOperator(
        task_id="LoadProducto",
        python_callable=LoadProducto,
        )

    PythonLoad2 = PythonOperator(
        task_id="LoadSucursal",
        python_callable=LoadSucursal,
        )

    PythonLoad3 = PythonOperator(
        task_id="LoadPrecios",
        python_callable=LoadPrecios,
        )
    
    FinishETL= EmptyOperator(
    task_id = 'FinishETL',
    dag = dag
    )

    SqlLoad = PythonOperator(
    task_id="SQLUploadProducto",
    python_callable=UploadAll,
    )

    FinishSQLLoading = EmptyOperator(
        task_id = 'FinishSQLLoading',
        dag = dag
        )

    CheckWithQuery = PythonOperator(
        task_id="CheckWithQuery",
        python_callable=MakeQuery,
    )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


StartPipeline >> [PythonLoad1, PythonLoad2, PythonLoad3] >> FinishETL

FinishETL >> SqlLoad >> FinishSQLLoading

FinishSQLLoading >> CheckWithQuery >> FinishPipeline