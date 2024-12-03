from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pdfplumber
from docx import Document
from docx.shared import Inches
import os

default_args = {
    'owner': 'Chonakan',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

MINIO_BUCKET_NAME = Variable.get("bucket_name")
MINIO_BUCKET_NAME_DOCX = Variable.get("bucket_name_docx")
LOCAL_PATH_PDF = "./include/course.pdf"
LOCAL_PATH_DOCX = "./include/course.docx"
AWS_CONN_ID = "minio_conn"

def test_bucket_connection():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    objects = hook.list_keys(bucket_name=MINIO_BUCKET_NAME)
    print("Connected to 'cdti-policies' bucket in MinIO successfully...")
    print(objects)

def ingest_docs():
    file_key = "course.pdf"
    local_path = f"/usr/local/airflow/include/{file_key}"
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    try:
        s3_object = s3_hook.get_key(key=file_key, bucket_name=MINIO_BUCKET_NAME)
        if s3_object:
            with open(local_path, "wb") as f:
                s3_object.download_fileobj(f)
            print(f"Downloaded {file_key} into local environment at {local_path}")
        else:
            print(f"File '{file_key}' not found in bucket '{MINIO_BUCKET_NAME}'.")
    except Exception as e:
        print(f"Error downloading file '{file_key}': {e}")


def convertor():
    with pdfplumber.open(LOCAL_PATH_PDF) as pdf:
        text = ""
        for page in pdf.pages:
            text += page.extract_text()
            
    document = Document()
    document.add_paragraph(text)
    document.save(LOCAL_PATH_DOCX)
    
def upload_file_to_bucket():
    OBJECT_NAME = "course.docx"
    
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_file(
            filename=LOCAL_PATH_DOCX,
            key=OBJECT_NAME,
            bucket_name=MINIO_BUCKET_NAME_DOCX,
            replace=True 
        )
        print(f"File {LOCAL_PATH_DOCX} uploaded successfully to {Variable.get('bucket_name_docx')}/{OBJECT_NAME}")
    
    except Exception as e:
        print(f"Error uploading file: {e}")

def cleanup_local_file():
    files_to_delete = [LOCAL_PATH_DOCX, LOCAL_PATH_PDF]
    for file_path in files_to_delete:
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            print("The file does not exist.")

with DAG(
    dag_id='pdfs_to_docx_convertor',
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
        bucket_name=MINIO_BUCKET_NAME,
        bucket_key='course.pdf',
        aws_conn_id=AWS_CONN_ID,
        mode='poke',
        poke_interval=10,
        timeout=60
    )
    
    ingest_documents = PythonOperator(
        task_id='ingest_documents',
        python_callable=ingest_docs
    )
    
    convertion = PythonOperator(
        task_id='convert_pdfs_to_docx',
        python_callable=convertor
    )
    
    upload_file = PythonOperator(
        task_id='upload_file_to_bucket',
        python_callable=upload_file_to_bucket
    )
    
    init_bucket_connection >> sensor_file_existance >> ingest_documents >> convertion >> upload_file