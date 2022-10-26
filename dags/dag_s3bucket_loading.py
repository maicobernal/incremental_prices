from datetime import datetime, timedelta
#from sys import get_asyncgen_hooks
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

#Check version of Airflow and Pip Amazon installed 
#Sensor to check if the file is loaded in the bucket
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

#Hook to connect to S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from functions import *
from etl import *

old_files = GetFiles()

#Set path for new files
dest_file_path = '/opt/airflow/minio_s3/'

# Default arguments
default_args = {
    'owner': 'Maico Bernal',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

# DAG
with DAG(
    dag_id='DAG_Minio_S3_Wait_for_File',
    start_date=datetime(2022, 10, 22),
    schedule_interval='@daily',
    default_args=default_args
) as dag:

    CheckS3 = S3KeySensor(

        #https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html
        task_id='sensor_minio_s3', #Name of task
        bucket_name='data', #Support relative or full path
        bucket_key='precio*', #Only if we didn't specify the full path, or we want to use UNIx style wildcards
        wildcard_match = True, #Set to true if we want to use wildcards
        aws_conn_id='minio_conn', #Name of the connection
        mode='poke', #Poke or reschedule
        poke_interval=5,
        timeout=30,

        #https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html?highlight=hooks#module-airflow.providers.amazon.aws.hooks.s3
        #download_async = get_hook(), #Get the hook
        #get_key = get_key(), #Get the key
        #download_async.download_file(key = get_key, path = dest_file_path), #Download the file
        #old_files.append(get_key),
        #print(f"File {get_key} downloaded")

    )

    #Check for new files and load them
    PythonAndSQLLoad = PythonOperator(
        task_id="LoadNewPrices",
        python_callable=LoadAndUploadNewPrecios,
        op_kwargs={'old_files': old_files, 'path': dest_file_path}
        )

    CheckNewPricesQuery = PythonOperator(
        task_id="CheckNewPricesQuery",
        python_callable=MakeQuery,
        )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


CheckS3 >> PythonAndSQLLoad >> CheckNewPricesQuery >> FinishPipeline