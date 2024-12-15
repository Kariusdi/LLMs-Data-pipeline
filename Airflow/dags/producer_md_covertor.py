from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime

# Define the dataset
dataset1 = Dataset('host.docker.internal:9000://cdti-policies/course.pdf')

with DAG(
    dag_id='producer_md_covertor',
    schedule=None,
    start_date=datetime(2023, 12, 1),
    catchup=False
) as dag:

    sensor = S3KeySensor(
        task_id='check_for_s3_update',
        aws_conn_id='minio_conn',
        bucket_name='cdti-policies',
        bucket_key='course.pdf',
        poke_interval=30,  # Check every 30 seconds
        timeout=600  # Timeout after 10 minutes
    )

    notify_airflow = EmptyOperator(
        task_id='mark_dataset_updated',
        outlets=[dataset1]
    )

    sensor >> notify_airflow
