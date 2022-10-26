from datetime import datetime, timedelta
#from sys import get_asyncgen_hooks
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.taskinstance import TaskInstance as ti
from tempfile import NamedTemporaryFile


#Check version of Airflow and Pip Amazon installed 
#Sensor to check if the file is loaded in the bucket
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

#Hook to connect to S3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from functions import *
from etl import *

#old_files = GetFiles()

#Set path for new files
dest_file_path = '/opt/airflow/dags/datasets/minio/'
dest_file_path_clean = '/opt/airflow/dags/datasets/minio/cleaned/'

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
        task_id='S3BucketSensorNewFiles', #Name of task
        bucket_name='data', #Support relative or full path
        bucket_key='precio*', #Only if we didn't specify the full path, or we want to use UNIx style wildcards
        wildcard_match = True, #Set to true if we want to use wildcards
        aws_conn_id='minio_conn', #Name of the connection
        mode='poke', #Poke or reschedule
        poke_interval=5,
        timeout=30
    )

    #Download the file from S3/Minio
    DownloadFileFromS3 = PythonOperator(
        task_id='DownloadFileFromS3',
        python_callable=DownloadAndRenameFile,
        op_kwargs={
            'bucket_name': 'data',
            'path': dest_file_path,
            }
    )

    FinishDownload = EmptyOperator(
        task_id='FinishDownload'
    )

    #Check for new files and load them
    PythonAndSQLLoad = PythonOperator(
        task_id="LoadNewPrices",
        python_callable=LoadAndUploadNewPrecios,
        op_kwargs={'path_new': dest_file_path}
        )

    #Make a Query to check if everything is fine
    CheckNewPricesQuery = PythonOperator(
        task_id="CheckNewPricesQuery",
        python_callable=MakeQuery,
        )

    FinishPipeline = EmptyOperator(
    task_id = 'FinishPipeline',
    dag = dag
    )


CheckS3 >> DownloadFileFromS3 >> FinishDownload
FinishDownload >> PythonAndSQLLoad >> CheckNewPricesQuery >> FinishPipeline