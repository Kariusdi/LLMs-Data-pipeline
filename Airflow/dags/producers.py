from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import DAG
from datetime import datetime, timedelta
from plugins.s3 import check_buckets_connection
import json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'Chonakan',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

MINIO_BUCKET_NAME_RAW = Variable.get("bucket_name_raw")
MINIO_BUCKET_NAME_TRANSFORMED = Variable.get("bucket_name_transformed")
BUCKET_LIST = [MINIO_BUCKET_NAME_RAW, MINIO_BUCKET_NAME_TRANSFORMED]

s3_hook = S3Hook(aws_conn_id='minio_conn')

def list_minio_files(bucket_name, prefix=None, ti=None):
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    if keys:
        print(f"Files in cdti-policies bucket /{prefix}: '{bucket_name}': {keys}")
    else:
        print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
    
    print(type(keys))
    Variable.set("bucket_files", json.dumps(keys))

with DAG(
    default_args=default_args,
    dag_id='syllabus_producer',
    schedule=None,
    catchup=False,
) as dag:
    
    buckets_connection_checker = PythonOperator(
        task_id='check_bucket_connection',
        python_callable=check_buckets_connection,
        op_kwargs={'bucket_list': BUCKET_LIST, 's3_hook': s3_hook}
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

    trigger_convertor = TriggerDagRunOperator(
        task_id="trigger_convertor",
        trigger_dag_id="md_convertor",
    )
    
    buckets_connection_checker >> list_files_task >> trigger_convertor
    
with DAG(
    default_args=default_args,
    dag_id='guide_producer',
    start_date=datetime(2023, 12, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    buckets_connection_checker = PythonOperator(
        task_id='check_bucket_connection',
        python_callable=check_buckets_connection,
        op_kwargs={'bucket_list': BUCKET_LIST, 's3_hook': s3_hook}
    )

    list_files_task = PythonOperator(
        task_id='guide_files_in_minio_bucket',
        python_callable=list_minio_files,
        op_kwargs={
            'bucket_name': MINIO_BUCKET_NAME_RAW,
            'prefix': 'guide/'
        },
        provide_context=True
    )

    trigger_convertor = TriggerDagRunOperator(
        task_id="trigger_convertor",
        trigger_dag_id="md_convertor",
    )
    
    buckets_connection_checker >> list_files_task >> trigger_convertor

with DAG(
    default_args=default_args,
    dag_id='rule_producer',
    start_date=datetime(2023, 12, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    buckets_connection_checker = PythonOperator(
        task_id='check_bucket_connection',
        python_callable=check_buckets_connection,
        op_kwargs={'bucket_list': BUCKET_LIST, 's3_hook': s3_hook}
    )

    list_files_task = PythonOperator(
        task_id='rule_files_in_minio_bucket',
        python_callable=list_minio_files,
        op_kwargs={
            'bucket_name': MINIO_BUCKET_NAME_RAW,
            'prefix': 'rule/'
        },
        provide_context=True
    )

    trigger_convertor = TriggerDagRunOperator(
        task_id="trigger_convertor",
        trigger_dag_id="md_convertor",
    )
    
    buckets_connection_checker >> list_files_task >> trigger_convertor