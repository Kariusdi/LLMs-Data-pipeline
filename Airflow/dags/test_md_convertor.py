# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.utils.trigger_rule import TriggerRule
# from airflow.operators.empty import EmptyOperator
# import os

# default_args = {
#     'owner': 'Chonakan',
#     'retries': 5,
#     'retry_delay': timedelta(minutes=1)
# }

# # Variables from Airflow Variable
# MINIO_BUCKET_NAME_RAW = Variable.get("bucket_name_raw")
# MINIO_BUCKET_NAME_TRANSFORMED = Variable.get("bucket_name_transformed")

# # Handling Files path
# BASE_LOCAL_PATH = "/usr/local/airflow/include/"
# LOCAL_PATH_PDF = BASE_LOCAL_PATH + "course.pdf"
# LOCAL_PATH_MD_UPDATE = BASE_LOCAL_PATH + "course_update.md"
# LOCAL_PATH_MD_ORIGINAL = BASE_LOCAL_PATH + "course.md"
# LOCAL_PATH_DELTA = BASE_LOCAL_PATH + "delta.bsdiff"
# BUCKET_FOLDER = "/cpe/"
# FILE_KEY_RAW = "course.pdf"
# FILE_KEY_TRANSFORMED = "course.md"

# # Bucket Connections
# S3_CONN_ID = "minio_conn"
# s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

# def buckets_connection_checker(**kwargs):
#     print("Checking bucket connections")

# def is_file_readable(**kwargs):
#     # Simulate file readability check
#     # Replace this logic with your actual implementation
#     if os.path.exists(LOCAL_PATH_PDF):
#         return "set_starter_page"
#     else:
#         return "ocr_operation"

# def raw_file_existance_sensor(**kwargs):
#     print("Checking raw file existence")

# def transformed_file_existance_sensor(**kwargs):
#     print("Checking transformed file existence")

# def ingest_document(**kwargs):
#     print("Ingesting raw document")

# def set_starter_page_number(**kwargs):
#     print("Setting starter page number")

# def convert_pdf_to_markdown(**kwargs):
#     print("Converting PDF to Markdown")

# def clean_md_file(**kwargs):
#     print("Cleaning Markdown file")

# def ingest_transformed_document(**kwargs):
#     print("Ingesting transformed document")

# def create_delta_file(**kwargs):
#     print("Creating delta file")

# def md_not_found(**kwargs):
#     print("Markdown not found")

# def upload_baseline_file(**kwargs):
#     print("Uploading baseline file")

# def ocr_operation(**kwargs):
#     print("Performing OCR")

# def generate_binary_delta():
#     print("Generate binary delta")

# with DAG(
#     dag_id='test_md_convertor',
#     start_date=datetime(2024, 11, 30),
#     schedule_interval='@daily',
#     catchup=False,
#     default_args=default_args
# ) as dag:
    
#     buckets_connection_checker1 = PythonOperator(
#         task_id='check_bucket_connection',
#         python_callable=buckets_connection_checker
#     )
    
#     raw_file_existance_sensor1 = S3KeySensor(
#         task_id='sensor_raw_file_existance',
#         bucket_name=MINIO_BUCKET_NAME_RAW,
#         bucket_key='course.pdf',
#         aws_conn_id=S3_CONN_ID,
#         mode='poke',
#         poke_interval=3,
#         timeout=15
#     )
    
#     transform_file_existance_sensor1 = S3KeySensor(
#         task_id='sensor_transform_file_existance',
#         bucket_name=MINIO_BUCKET_NAME_RAW,
#         bucket_key='course2.pdf',
#         aws_conn_id=S3_CONN_ID,
#         mode='poke',
#         poke_interval=3,
#         timeout=15,
#         soft_fail=True,
#     )
    
#     branch_task = BranchPythonOperator(
#         task_id='branch_based_on_readable',
#         python_callable=is_file_readable
#     )
    
#     set_starter_page_number1 = PythonOperator(
#         task_id='set_starter_page',
#         python_callable=set_starter_page_number
#     )
    
#     convert_pdf_to_markdown1 = PythonOperator(
#         task_id='pdf_to_markdown',
#         python_callable=convert_pdf_to_markdown
#     )
    
#     clean_md_file1 = PythonOperator(
#         task_id='clean_markdown_file',
#         python_callable=clean_md_file
#     )
    
#     ocr_operation1 = PythonOperator(
#         task_id='ocr_operation',
#         python_callable=ocr_operation
#     )
    
#     generate_delta_file = PythonOperator(
#         task_id='generate_delta_file',
#         python_callable=generate_binary_delta
#     )
    
#     upload_delta_file = PythonOperator(
#         task_id='upload_delta_to_bucket',
#         python_callable=upload_baseline_file,
#         trigger_rule=TriggerRule.ALL_SUCCESS
#     )
    
#     upload_baseline_file1 = PythonOperator(
#         task_id='upload_baseline_to_bucket',
#         python_callable=upload_baseline_file,
#         trigger_rule=TriggerRule.ALL_SUCCESS
#     )
    
#     ingest_raw_document = PythonOperator(
#         task_id='ingest_raw_document',
#         python_callable=ingest_document,
#         dag=dag
#     )
    
#     ingest_transformed_document1 = PythonOperator(
#         task_id='ingest_transformed_document',
#         python_callable=ingest_document,
#         trigger_rule=TriggerRule.ONE_SUCCESS,
#         dag=dag
#     )
    
#     md_not_found1 = EmptyOperator(task_id="md_not_found", trigger_rule=TriggerRule.ALL_SKIPPED)

    
    
#     # Define the DAG flow
#     buckets_connection_checker1 >> raw_file_existance_sensor1
#     buckets_connection_checker1 >> transform_file_existance_sensor1
    
#     raw_file_existance_sensor1 >> ingest_raw_document >> branch_task
#     transform_file_existance_sensor1 >> ingest_transformed_document1 >> generate_delta_file >> upload_delta_file
#     transform_file_existance_sensor1 >> md_not_found1 >> upload_baseline_file1
    
#     # Branching paths
#     branch_task >> set_starter_page_number1 >> convert_pdf_to_markdown1 >> clean_md_file1
#     branch_task >> ocr_operation1
    
#     # Common downstream tasks
#     clean_md_file1 >> generate_delta_file >> upload_delta_file
#     clean_md_file1 >> upload_baseline_file1
