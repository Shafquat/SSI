from datetime import datetime, timedelta
 from airflow import DAG
 from airflow.operators.python import PythonOperator
 from airflow.providers.amazon.aws.hooks.s3 import S3Hook
 import os
 
 LOCAL_FOLDER = "D:/division_data"
 S3_BUCKET = "dataproject-redshifttempdatabucket-s6sp8tfn9hgn"
 S3_KEY_PREFIX = 'data/'
 
 # Function to list files in your local folder
 def list_files_in_local_folder():
     files = [f for f in os.listdir(LOCAL_FOLDER) if os.path.isfile(os.path.join(LOCAL_FOLDER, f))]
     return files
 
 # Function to check if file exists in S3 and upload if not
 def upload_files_to_s3(**kwargs):
     ti = kwargs['ti']
     files = ti.xcom_pull(task_ids='list_local_files')
     s3_hook = S3Hook(aws_conn_id='aws_default')
 
     for file in files:
         s3_key = f"{S3_KEY_PREFIX}{file}"
         if not s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET):
             file_path = os.path.join(LOCAL_FOLDER, file)
             s3_hook.load_file(filename=file_path, key=s3_key, bucket_name=S3_BUCKET, replace=False)
             print(f"Uploaded {file} to S3 bucket {S3_BUCKET}/{s3_key}")
         else:
             print(f"{file} already exists in S3 bucket {S3_BUCKET}/{s3_key}")
 
 default_args = {
     'owner': 'airflow',
     'depends_on_past': False,
     'email_on_failure': False,
     'email_on_retry': False,
     'retries': 1,
     'retry_delay': timedelta(minutes=1),
 }
 
 with DAG(
     'upload_files_to_s3_dag',
     default_args=default_args,
     description='A DAG to upload files from a local folder to S3 if not already present',
     schedule_interval=timedelta(days=1),
     start_date=datetime(2024, 12, 24),
     catchup=False,
     tags=['example', 's3'],
 ) as dag:
 
     list_local_files = PythonOperator(
         task_id='list_local_files',
         python_callable=list_files_in_local_folder,
     )
 
     upload_to_s3 = PythonOperator(
         task_id='upload_files_to_s3',
         python_callable=upload_files_to_s3,
         provide_context=True,
     )
 
     list_local_files >> upload_to_s3