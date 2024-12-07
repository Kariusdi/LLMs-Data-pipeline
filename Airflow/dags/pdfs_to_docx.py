from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.bash import BashOperator
import pdfplumber
from docx import Document
from docx.shared import Inches
import os
from botocore.exceptions import ClientError
import bsdiff4

default_args = {
    'owner': 'Chonakan',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

MINIO_BUCKET_NAME = Variable.get("bucket_name")
MINIO_BUCKET_NAME_DOCX = Variable.get("bucket_name_docx")
LOCAL_PATH_PDF = "/usr/local/airflow/include/course.pdf"
LOCAL_PATH_DOCX_UPDATE = "/usr/local/airflow/include/course_update.docx"
LOCAL_PATH_DOCX_ORIGINAL = "/usr/local/airflow/include/course.docx"
LOCAL_PATH_DELTA = "/usr/local/airflow/include/delta.bsdiff"
AWS_CONN_ID = "minio_conn"

s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

def test_bucket_connection():
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    objects = hook.list_keys(bucket_name=MINIO_BUCKET_NAME)
    print("Connected to 'cdti-policies' bucket in MinIO successfully...")
    print(objects)

def ingest_docs(file_key: str, bucketName: str):
    local_path = f"/usr/local/airflow/include/course.pdf"
    try:
        s3_object = s3_hook.get_key(key=file_key, bucket_name=bucketName)
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
    document.save(LOCAL_PATH_DOCX_UPDATE)

def generate_binary_delta(baseline_file: str, update_file: str, delta_file: str):
    bsdiff4.file_diff(baseline_file, update_file, delta_file)
    
def upload_file_to_bucket():
    baseline_key = "/cpe/course.docx"
    delta_key = "/cpe/delta.bsdiff"
    
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_client = s3_hook.get_conn()
    
    # Check if the baseline file exists in the bucket
    objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET_NAME_DOCX, Prefix=baseline_key)
    
    if 'Contents' in objects:  # Baseline file exists
        print(f"File {baseline_key} already exists in the bucket, which is the baseline document.")
        
        # Generate and upload delta file
        ingest_docs(file_key=baseline_key, bucketName=MINIO_BUCKET_NAME_DOCX)  # Optional processing step
        generate_binary_delta(LOCAL_PATH_DOCX_ORIGINAL, LOCAL_PATH_DOCX_UPDATE, LOCAL_PATH_DELTA)  # Create delta file
        
        s3_hook.load_file(
            filename=LOCAL_PATH_DELTA,
            key=delta_key,
            bucket_name=MINIO_BUCKET_NAME_DOCX,
            replace=True  # Overwrite if it exists
        )
        print(f"Delta file {LOCAL_PATH_DELTA} uploaded successfully to {Variable.get('bucket_name_docx')}/{delta_key}")
    else:  # Baseline file does not exist
        print(f"Baseline file {baseline_key} does not exist in the bucket. Uploading baseline file as the initial version.")
        
        # Upload baseline file
        s3_hook.load_file(
            filename=LOCAL_PATH_DOCX_ORIGINAL,
            key=baseline_key,
            bucket_name=MINIO_BUCKET_NAME_DOCX,
            replace=True  # Overwrite if it exists
        )
        print(f"Baseline file {LOCAL_PATH_DOCX_ORIGINAL} uploaded successfully to {Variable.get('bucket_name_docx')}/{baseline_key}")


def cleanup_local_file():
    files_to_delete = [LOCAL_PATH_PDF, LOCAL_PATH_DOCX_ORIGINAL, LOCAL_PATH_DOCX_UPDATE, LOCAL_PATH_DELTA]
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
        python_callable=ingest_docs,
        op_args=["course.pdf", MINIO_BUCKET_NAME]
    )
    
    convertion = PythonOperator(
        task_id='convert_pdfs_to_docx',
        python_callable=convertor
    )
    
    upload_file = PythonOperator(
        task_id='upload_file_to_bucket',
        python_callable=upload_file_to_bucket
    )
    
    cleanup_file = PythonOperator(
        task_id='cleanup_local_file',
        python_callable=cleanup_local_file
    )
    
    init_bucket_connection >> sensor_file_existance >> ingest_documents >> convertion >> upload_file >> cleanup_file