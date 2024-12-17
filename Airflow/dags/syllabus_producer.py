from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow import DAG
from datetime import datetime, timedelta
from plugins.s3 import check_buckets_connection

default_args = {
    'owner': 'Chonakan',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

MINIO_BUCKET_NAME_RAW = Variable.get("bucket_name_raw")
s3_hook = S3Hook(aws_conn_id='minio_conn')

def list_minio_files(bucket_name, prefix=None, ti=None):
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    if keys:
        print(f"Files in cdti-policies bucket /syllabus: '{bucket_name}': {keys}")
        ti.xcom_push(
            key="bucket_files",
            value=keys
        )
    else:
        print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    return keys

dataset1 = Dataset('host.docker.internal:9000://cdti-policies/')

with DAG(
    default_args=default_args,
    dag_id='syllabus_producer',
    start_date=datetime(2023, 12, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    buckets_connection_checker = PythonOperator(
        task_id='check_bucket_connection',
        python_callable=check_buckets_connection,
        op_kwargs={'bucket_list': [MINIO_BUCKET_NAME_RAW], 's3_hook': s3_hook}
    )

    list_files_task = PythonOperator(
        task_id='syllabus_files_in_minio_bucket',
        python_callable=list_minio_files,
        op_kwargs={
            'bucket_name': MINIO_BUCKET_NAME_RAW,
            'prefix': 'syllabus/'
        },
        provide_context=True
    )
    
    notify_airflow = EmptyOperator(
        task_id='mark_dataset_updated',
        outlets=[dataset1]
    )

    buckets_connection_checker >> list_files_task >> notify_airflow