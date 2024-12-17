from datetime import datetime, timedelta
import pathlib
from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
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
BUCKET_FOLDER = "/syllabus/"
FILE_KEY_RAW = BUCKET_FOLDER + "course.pdf"
FILE_KEY_TRANSFORMED = BUCKET_FOLDER + "course.md"
BASELINE_KEY = BUCKET_FOLDER + "course.md"
DELTA_KEY = BUCKET_FOLDER + "delta.bsdiff"

# Bucket Connections
S3_CONN_ID = "minio_conn"
s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

from airflow.models import XCom
from airflow.utils.session import provide_session

@provide_session
def get_xcom_values_for_key(dag_ids, key, execution_date, session=None):
    """
    Fetches XCom values for a given key from multiple DAGs.
    """
    results = session.query(XCom).filter(
        XCom.dag_id.in_(dag_ids),  # Filter for multiple DAGs
        XCom.key == key,          # Filter for the specific key
        XCom.execution_date == execution_date
    ).all()
    
    print(results)
    
    return [result.value for result in results]  # Return a list of values

def process_xcom_values(**kwargs):
    dag_ids = ['syllabus_producer', 'guide_producer', 'rule_producer']
    key = 'bucket_files'
    execution_date = kwargs['execution_date']
    values = get_xcom_values_for_key(dag_ids, key, execution_date)
    print(f"Consolidated values: {values}")

dataset1 = Dataset('host.docker.internal:9000://cdti-policies/')

with DAG(
    dag_id='test',
    start_date=datetime(2024, 12, 12),
    # schedule_interval='@daily',
    schedule=[dataset1],
    catchup=False,
    default_args=default_args
) as dag:
    
    buckets_connection_checker = PythonOperator(
        task_id='check_bucket_connection',
        python_callable=process_xcom_values
    )
    
    buckets_connection_checker
