from datetime import datetime, timedelta
import pathlib
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import fitz
import os
import pymupdf4llm
import bsdiff4
import re

default_args = {
    'owner': 'Chonakan',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Variables from Airflow Variable
MINIO_BUCKET_NAME_RAW = Variable.get("bucket_name_raw")
MINIO_BUCKET_NAME_TRANSFORMED = Variable.get("bucket_name_transformed")

# Handling Files path
BASE_LOCAL_PATH = "/usr/local/airflow/include/"
LOCAL_PATH_PDF = BASE_LOCAL_PATH + "course.pdf"
LOCAL_PATH_MD_UPDATE = BASE_LOCAL_PATH + "course_update.md"
LOCAL_PATH_MD_ORIGINAL = BASE_LOCAL_PATH + "course.md"
LOCAL_PATH_DELTA = BASE_LOCAL_PATH + "delta.bsdiff"
BUCKET_FOLDER = "/cpe/"
FILE_KEY_RAW = "course.pdf"
FILE_KEY_TRANSFORMED = "course.md"
BASELINE_KEY = BUCKET_FOLDER + "course.docx"
DELTA_KEY = BUCKET_FOLDER + "delta.bsdiff"

# Bucket Connections
S3_CONN_ID = "minio_conn"
s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

def check_buckets_connection():
    bucket_list = [MINIO_BUCKET_NAME_RAW, MINIO_BUCKET_NAME_TRANSFORMED]
    for bucket_name in bucket_list:
        s3_object = s3_hook.list_keys(bucket_name=bucket_name)
        print(f"Connected to the {bucket_name} in MinIO successfully...", s3_object)

def ingest_document(file_key: str, bucketName: str, localPath: str):
    try:
        s3_object = s3_hook.get_key(key=file_key, bucket_name=bucketName)
        if s3_object:
            with open(localPath, "wb") as f:
                s3_object.download_fileobj(f)
            print(f"Downloaded {file_key} into local environment at {localPath}")
        else:
            print(f"File '{file_key}' not found in bucket '{bucketName}'.")
    except Exception as e:
        print(f"Error downloading file '{file_key}': {e}")

def set_starter_page(pdf_path):
    pdf_document = fitz.open(pdf_path)

    print("Extracting footers from the PDF:\n")
    count_one = 0

    for page_number in range(len(pdf_document)):
        page = pdf_document[page_number]
        text_blocks = page.get_text("blocks")

        page_height = page.rect.height
        footer_threshold = page_height * 0.90 # 10 % from bottom

        footer_number = [
            block[4] for block in text_blocks if block[1] > footer_threshold
        ]

        print(f"Page {page_number + 1}:")
        if footer_number:
            # print(f"  Footer: {' | '.join(footer_number).strip()}")
            if int(footer_number[0]) == 1:
                count_one += 1
            if count_one == 2:
                print(f"Returning page {page_number} as starting point.")
                pdf_document.close()
                return page_number
        # else:
        #     print("  Footer: None")
        # print("-" * 50)

    pdf_document.close()
    return 0

def pdf_to_markdown(pdf_path, output_md_path, start_page=0):
    pdf_document = fitz.open(pdf_path)
    pages_to_include = []
    print("----->>>>>> ",pdf_document)

    for page_number in range(start_page, len(pdf_document)):
        page = pdf_document[page_number]
        text = page.get_text()
        if text.strip():
            pages_to_include.append(page_number)
    pdf_document.close()
    md_text = pymupdf4llm.to_markdown(pdf_path, pages=pages_to_include)
    pathlib.Path(output_md_path).write_bytes(md_text.encode())
    print(f"Markdown file saved to {output_md_path}")
    
def check_readable_file(pdf_path):
    pdf_document = fitz.open(pdf_path)
    pages_to_include = []
    for page_number in range(0, len(pdf_document)):
        page = pdf_document[page_number]
        text = page.get_text()
        if text.strip():
            pages_to_include.append(page_number)
    if not pages_to_include:
        print("No pages with readable text were found.")
        return "ocr"
    return "set_starter_page"
    
def clean_markdown_file(input_md_path, output_md_path):
    # Read the input Markdown file
    with open(input_md_path, 'r', encoding='utf-8') as f:
        md_text = f.read()

    # Clean the Markdown content
    md_text = clean_markdown(md_text)

    # Write the cleaned content to the output file
    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(md_text)

    print(f"Cleaned Markdown file saved to {output_md_path}")

def clean_markdown(md_text):
    md_text = re.sub(r'\s*(\d+)\s*(?=\n)', '\n', md_text)
    md_text = re.sub(r'#### \s*(\d+)\s*(?=\n)', '\n', md_text)
    return md_text

def ocr():
    print("Use OCR To Transform PDF...")

def generate_binary_delta(baseline_file: str, update_file: str, delta_file: str):
    bsdiff4.file_diff(baseline_file, update_file, delta_file)

def upload_file_to_bucket(fileKey: str, bucketName: str, uploadFile: str):
    s3_hook.load_file(
        filename=uploadFile,
        key=fileKey,
        bucket_name=bucketName,
        replace=True
    )
    print(f"Baseline file {LOCAL_PATH_MD_ORIGINAL} uploaded successfully to {Variable.get('bucket_name_transformed')}/{fileKey}")


with DAG(
    dag_id='md_convertor',
    start_date=datetime(2024, 12, 12),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:
    
    buckets_connection_checker = PythonOperator(
        task_id='check_bucket_connection',
        python_callable=check_buckets_connection
    )
    
    raw_file_existance_sensor = S3KeySensor(
        task_id='sensor_raw_file_existance',
        bucket_name=MINIO_BUCKET_NAME_RAW,
        bucket_key='course.pdf',
        aws_conn_id=S3_CONN_ID,
        mode='poke',
        poke_interval=3,
        timeout=15
    )
    
    transformed_file_existance_sensor = S3KeySensor(
        task_id='sensor_transformed_file_existance',
        bucket_name=MINIO_BUCKET_NAME_TRANSFORMED,
        bucket_key='course.md',
        aws_conn_id=S3_CONN_ID,
        mode='poke',
        poke_interval=3,
        timeout=15,
        soft_fail=True,
    )
    
    ingest_raw_document = PythonOperator(
        task_id='ingest_raw_document',
        python_callable=ingest_document,
        op_kwargs={'file_key': FILE_KEY_RAW, 'bucketName': MINIO_BUCKET_NAME_RAW, 'localPath': LOCAL_PATH_PDF}
    )
    
    ingest_transformed_document = PythonOperator(
        task_id='ingest_transformed_document',
        python_callable=ingest_document,
        op_kwargs={'file_key': FILE_KEY_TRANSFORMED, 'bucketName': MINIO_BUCKET_NAME_TRANSFORMED, 'localPath': LOCAL_PATH_MD_ORIGINAL},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    is_file_readable = PythonOperator(
        task_id='check_readable_file',
        python_callable=check_readable_file,
        op_kwargs={'pdf_path': LOCAL_PATH_PDF}
    )
    
    set_starter_page_number = PythonOperator(
        task_id='set_starter_page',
        python_callable=set_starter_page,
        op_kwargs={'pdf_path': LOCAL_PATH_PDF}
    )
    
    convert_pdf_to_markdown = PythonOperator(
        task_id='pdf_to_markdown',
        python_callable=pdf_to_markdown,
        op_kwargs={'pdf_path': LOCAL_PATH_PDF, 'output_md_path': LOCAL_PATH_MD_UPDATE, 'start_page': "{{ ti.xcom_pull(task_ids='set_starter_page') }}"}
    )
    
    ocr_operation = PythonOperator(
        task_id='ocr_operation',
        python_callable=ocr
    )
    
    clean_md_file = PythonOperator(
        task_id='clean_markdown_file',
        python_callable=clean_markdown_file,
        op_kwargs={'input_md_path': LOCAL_PATH_MD_ORIGINAL, 'output_md_path': LOCAL_PATH_MD_ORIGINAL}
    )
    
    generate_delta_file = PythonOperator(
        task_id='generate_delta_file',
        python_callable=generate_binary_delta,
        op_kwargs={'baseline_file': LOCAL_PATH_MD_ORIGINAL, 'update_file': LOCAL_PATH_MD_UPDATE, 'delta_file': LOCAL_PATH_DELTA}
    )
    upload_delta_file = PythonOperator(
        task_id='upload_delta_to_bucket',
        python_callable=upload_file_to_bucket,
        op_kwargs={'fileKey': DELTA_KEY, 'bucketName': MINIO_BUCKET_NAME_TRANSFORMED, 'uploadFile': LOCAL_PATH_DELTA},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    upload_baseline_file = PythonOperator(
        task_id='upload_baseline_to_bucket',
        python_callable=upload_file_to_bucket,
        op_kwargs={'fileKey': BASELINE_KEY, 'bucketName': MINIO_BUCKET_NAME_TRANSFORMED, 'uploadFile': LOCAL_PATH_MD_UPDATE},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    md_not_found = EmptyOperator(task_id="md_not_found", trigger_rule=TriggerRule.ALL_SKIPPED)
    
    # Define the DAG flow
    buckets_connection_checker >> raw_file_existance_sensor
    buckets_connection_checker >> transformed_file_existance_sensor
    
    raw_file_existance_sensor >> ingest_raw_document >> is_file_readable
    transformed_file_existance_sensor >> ingest_transformed_document >> generate_delta_file >> upload_delta_file
    transformed_file_existance_sensor >> md_not_found >> upload_baseline_file
    
    
    is_file_readable >> set_starter_page_number >> convert_pdf_to_markdown >> clean_md_file
    is_file_readable >> ocr_operation
    
    clean_md_file >> generate_delta_file >> upload_delta_file
    clean_md_file >> upload_baseline_file
