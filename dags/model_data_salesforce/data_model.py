from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import os

owner_process = 'oquintero'
connection_swn = 'snowflake_connect'
DAG_PATH = os.path.dirname(os.path.realpath(__file__))

default_args = {
    'owner': 'oquintero',
    'start_date': datetime(2026, 1, 1),
    'retries': 1
}

with DAG(
    dag_id=owner_process.upper() + '_' + 'salesforce_data_modeling',
    default_args=default_args,
    schedule_interval='0 5,8 * * *',
    template_searchpath=[DAG_PATH],
    catchup=False,
    tags=['model data', 'salesforce']
) as dag:

    # 1. Procesar Dimensiones SCD2 en paralelo
    t1 = SnowflakeOperator(
        task_id='dim_account_master',
        snowflake_conn_id=connection_swn,
        sql='queries/ACCOUNT_MASTER_DIM.sql'
    )

    t2 = SnowflakeOperator(
        task_id='dim_product_catalog',
        snowflake_conn_id=connection_swn,
        sql='queries/PRODUCT_CATALOG_DIM.sql'
    )

    t3 = SnowflakeOperator(
        task_id='dim_sales_rep',
        snowflake_conn_id=connection_swn,
        sql='queries/SALES_REP_DIM.sql'
    )

    t4 = SnowflakeOperator(
        task_id='fact_sales_orders',
        snowflake_conn_id=connection_swn,
        sql='queries/SALES_ORDER_FACT.sql'
    )

    [t1, t2, t3] >> t4