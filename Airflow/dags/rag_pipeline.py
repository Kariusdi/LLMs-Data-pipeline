from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
import chromadb

default_args = {
    'owner': 'Chonakan',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

MINIO_BUCKET_NAME_DOCX = Variable.get("bucket_name_docx")

def test_bucket_connection():
    hook = S3Hook(aws_conn_id='minio_conn')
    objects = hook.list_keys(bucket_name=MINIO_BUCKET_NAME_DOCX)
    print("Connected to 'cdti-documents' bucket in MinIO successfully...")
    print(objects)

def ingest_docs():
    file_key = "course.pdf"
    local_path = f"/usr/local/airflow/include/{file_key}"
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    try:
        s3_object = s3_hook.get_key(key=file_key, bucket_name=MINIO_BUCKET_NAME_DOCX)
        if s3_object:
            with open(local_path, "wb") as f:
                s3_object.download_fileobj(f)
            print(f"Downloaded {file_key} into local environment at {local_path}")
        else:
            print(f"File '{file_key}' not found in bucket '{MINIO_BUCKET_NAME_DOCX}'.")
    except Exception as e:
        print(f"Error downloading file '{file_key}': {e}")

def transform_and_store_to_vector_db():
    print("Transform and store to VectorDB operation.")

with DAG(
    dag_id='rag_pipeline',
    start_date=datetime(2024, 11, 30),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:
    
    init_bucket_connection = PythonOperator(
        task_id='test_bucket_connection',
        python_callable=test_bucket_connection
    )
    
    sensor_file_existance = S3KeySensor(
        task_id='sensor_file_existance',
        bucket_name=MINIO_BUCKET_NAME_DOCX,
        bucket_key='course.docx',
        aws_conn_id='minio_conn',
        mode='poke',
        poke_interval=10,
        timeout=60
    )
    
    ingest_documents = PythonOperator(
        task_id='ingest_documents',
        python_callable=ingest_docs
    )
    
    transform_and_load_documents = PythonOperator(
        task_id='transform_and_load_documents',
        python_callable=transform_and_store_to_vector_db
    )
    
    init_bucket_connection >> sensor_file_existance >> ingest_documents >> transform_and_load_documents