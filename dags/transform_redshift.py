from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import numpy as np

# Constants
S3_BUCKET = 'dataproject-redshifttempdatabucket-s6sp8tfn9hgn'
S3_KEY = 'data/' 
REDSHIFT_TABLE = 'divsion_data'
AWS_CONN_ID = 'AWS_CONN'
REDSHIFT_CONN_ID = 'dataproject'

# Function to perform transformations
def transform_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_KEY)

    dfs = []
    for file in files:
        url = s3_hook.generate_presigned_url(S3_BUCKET, file, expiration=600)
        df = pd.read_csv(url)
        dfs.append(df)
   
    # Union all dataframes
    full_df = pd.concat(dfs, ignore_index=True)
   
    # Transformations
    full_df['Days to Completion'] = (pd.to_datetime(full_df['End Date']) - pd.to_datetime(full_df['Start Date'])).dt.days
    full_df['Business Days to Completion'] = np.busday_count(full_df['Start Date'].values.astype('datetime64[D]'), full_df['End Date'].values.astype('datetime64[D]'))
    full_df['Service Division Owner'] = full_df['Service Division Owner'].replace('and', '&', regex=True)
   
    # Save the transformed dataframe to a CSV file
    transformed_file_path = '/tmp/transformed_data.csv'
    full_df.to_csv(transformed_file_path, index=False, header=False)
   
    # Upload the transformed data to S3 for Redshift COPY
    transformed_s3_key = f"{S3_KEY}transformed/transformed_data.csv"
    s3_hook.load_file(filename=transformed_file_path, bucket_name=S3_BUCKET, replace=True, key=transformed_s3_key)
   
    return transformed_s3_key

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'transform_redshift',
    default_args=default_args,
    description='Transform S3 data and load into Redshift',
    schedule_interval='@daily',
    catchup=False,
    tags=['s3', 'transform', 'redshift'],
) as dag:

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_to_redshift = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        s3_bucket=S3_BUCKET,
        s3_key="{{ task_instance.xcom_pull(task_ids='transform_data') }}",
        schema='public',
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
        aws_conn_id=AWS_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID,
    )

    transform_data_task >> load_to_redshift