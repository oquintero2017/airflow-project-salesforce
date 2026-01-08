from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import os

def validate_sale_data(file_path):
    df = pd.read_csv(file_path)
    if 'quantity' not in df.columns or 'unit_price' not in df.columns:
        raise ValueError("Columns 'quantity' and 'unit_price' must exist in the CSV")
    
    negative_quantity = df[df['quantity'] < 0]
    negative_unit_price = df[df['unit_price'] < 0]

    if not negative_quantity.empty or not negative_unit_price.empty:
        print(f"Data Quality Error: Found negative values in the quantity column rows:")
        print(negative_quantity[['order_id', 'quantity', 'unit_price']])
        print(negative_unit_price[['order_id', 'quantity', 'unit_price']])
        raise ValueError("Data quality validation failed: Negative values found in quantity or unit_price columns")
    
    return True

with DAG(
    dag_id=owner_process.upper() + '_salesforce_data_validation',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    template_searchpath=[DAG_PATH],
    catchup=False,
    tags=['validation', 'salesforce']
) as dag:
    
     
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    validate_task