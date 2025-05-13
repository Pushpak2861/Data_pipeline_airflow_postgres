from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python import PythonOperator
import requests
import io
import pandas as pd
import boto3
import os
from dotenv import load_dotenv

load_dotenv()
default_args = {
    "owner":"spr_groups",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

local_wokflow = DAG(
    dag_id="python_test_airflow",
    default_args=default_args,
    start_date=datetime(2024,1,1),
    schedule="0 6 2 * *",
    catchup=True
)

def greet(**kwargs):
    print(f"ds:{kwargs['ds']} dag_id: {kwargs['dag'].dag_id}")


def get_data(url,ti):

    aws_access_key_id = os.environ["AWS_ACCESS_KEY"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    region_name = os.environ["AWS_REGION_NAME"]

    response = requests.get(url)
    buffer = io.BytesIO(response.content)

    parquet_data = pd.read_parquet(buffer)
    csv_buffer = io.StringIO()

    parquet_data.to_csv(csv_buffer , index=False)

    csv_buffer.seek(0)

    bucket_name = os.environ["BUCKET"]
    key_name = os.environ["KEY"]

    if not bucket_name or not key_name:
        raise ValueError("BUCKET name or KEY name is missing or mismatching")

    s3 = boto3.client("s3",aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)
    s3_response = s3.put_object(Bucket = bucket_name , Key = key_name , Body = csv_buffer.getvalue().encode("utf-8"))

    print(s3_response["ETag"])
    ti.xcom_push(key = "bucket_name" , value = bucket_name)
    ti.xcom_push(key = "key_name" , value = key_name)
    print(f"bucket name {bucket_name} and key name {key_name} pushed successfully to xcom")

with local_wokflow:
    gre = PythonOperator(
        task_id = "greet",
        python_callable=greet
    )

    get_ = PythonOperator(
        task_id = "get_data",
        python_callable=get_data,
        op_kwargs={"url":"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"}
    ) 

    gre >> get_ 