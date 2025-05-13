from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import os 
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime ,timedelta
import io
from sqlalchemy import create_engine
from tqdm import tqdm

load_dotenv()


default_args = {
    "owner": "spr_groups",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

local_worflow = DAG(
    dag_id="push_to_postgres",
    default_args=default_args,
    schedule="0 6 2 * *",
    start_date=datetime(2025,1,1)
)

def push_data(ti):
    bucket_name = ti.xcom_pull(dag_id = "python_test_airflow" , task_ids = "get_data" , key = "bucket_name" , run_id = "manual__2025-05-13T15:13:58.034383+00:00")
    key_name = ti.xcom_pull(dag_id = "python_test_airflow" , task_ids = "get_data" , key = "key_name" , run_id = "manual__2025-05-13T15:13:58.034383+00:00")
    print(f"bucket_name {bucket_name} key_name {key_name}")

    aws_access_key_id = os.environ["AWS_ACCESS_KEY"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    region_name = os.environ["AWS_REGION_NAME"]
    host = os.environ["POSTGRES_HOST"]
    password = os.environ["POSTGRES_PASS"]

    s3 = boto3.client("s3" , aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)
    response = s3.get_object(Bucket = bucket_name , Key = key_name)
    data = response["Body"].read().decode("utf-8")

    #io.StringIO is to convert the string like object to file like object
    df_chunk = pd.read_csv(io.StringIO(data) , iterator= True , chunksize=10000)
    df = next(df_chunk)
    
    #engine creation
    eng = create_engine(f"postgresql://{host}:{password}@db_pgadmin:5432/ny_taxi")

    df.head(n=0).to_sql("trip_tracker" , con=eng , if_exists="replace")
    df.to_sql("trip_tracker" , con=eng , if_exists="append")

    for chunk in tqdm(df_chunk , desc="ingesting trip data to the table"):
        chunk.to_sql("trip_tracker" , con=eng , if_exists="append")
    
    


with local_worflow:
    push_ = PythonOperator(
        task_id = "push_data",
        python_callable=push_data
    )

    push_
