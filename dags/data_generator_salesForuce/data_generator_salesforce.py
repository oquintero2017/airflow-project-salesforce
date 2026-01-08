from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import random
import io
import os

# --- CONFIGURACIÓN ---
BUCKET_NAME = 'file-saleforce-test-s3' 
process_name = 'data_generator_salesforce'
owner_process = 'oquintero'
connection_s3 = 'aws_s3_file_salesforce'
# root_sql = '/opt/airflow/dags/data_generator_salesForuce/queries'
DAG_PATH = os.path.dirname(os.path.realpath(__file__))
sql_path = 'queries/refresh_external_table.sql'

def upload_to_s3(df, root, filename, append=False):
    csv_buffer = io.StringIO()
    
    if append:
        try:
            s3_hook = S3Hook(aws_conn_id=connection_s3)
            temp_file = f'/tmp/{filename}'
            s3_hook.download_file(key=f"{root}/{filename}", bucket_name=BUCKET_NAME, local_path=temp_file)
            existing_df = pd.read_csv(temp_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_csv(csv_buffer, index=False)
        except Exception as e:
            df.to_csv(csv_buffer, index=False)
    else:
        df.to_csv(csv_buffer, index=False)
    
    s3_hook = S3Hook(aws_conn_id=connection_s3)
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=f"{root}/{filename}",
        bucket_name=BUCKET_NAME,
        replace=True
    )

# ---Funtions to generate dummy data ---

def task_product_catalog():
    products_data = [
        ["P-1001", "Sales Cloud - Enterprise", "Sales Cloud", 15000],
        ["P-1002", "Sales Cloud - Pro", "Sales Cloud", 8000],
        ["S-2001", "Service Cloud - Unlimited", "Service Cloud", 18000],
        ["D-3001", "Data Cloud - Base", "Data Cloud", 25000],
        ["T-4001", "Tableau Creator", "Tableau", 840]
    ]
    df = pd.DataFrame(products_data, columns=['product_id', 'product_name', 'product_family', 'list_price'])
    upload_to_s3(df, 'product_catalog', 'product_catalog_extract.csv')

def task_sales_rep_master():
    reps_data = [
        ["SR-10", "Ana Flores", "Carlos Rey", "LATAM", "2022-03-01"],
        ["SR-11", "Bruno Costa", "Carlos Rey", "LATAM", "2023-05-15"],
        ["SR-12", "Dana White", "Julia Chen", "NORAM", "2021-11-01"],
        ["SR-13", "Evan Grant", "Julia Chen", "NORAM", "2022-08-20"],
        ["SR-14", "Fiona Lee", "Raj Patel", "EMEA", "2023-01-10"]
    ]
    df = pd.DataFrame(reps_data, columns=['sales_rep_id', 'rep_name', 'manager_name', 'territory', 'hire_date'])
    upload_to_s3(df, 'sales_rep_master', 'sales_rep_master_extract.csv')

def task_account_master():
    industries = ["Technology", "Finance", "Retail", "Healthcare", "Manufacturing", "Energy"]
    segments = ["Enterprise", "SMB", "Mid-Market"]
    accounts_data = []
    for i in range(1, 21):
        acc_id = f"A-{i:03d}"
        acc_name = f"Company {chr(64 + (i % 26 if i % 26 != 0 else 26))} {i}"
        industry = random.choice(industries)
        segment = random.choice(segments)
        accounts_data.append([acc_id, acc_name, industry, segment])
    df = pd.DataFrame(accounts_data, columns=['account_id', 'account_name', 'industry', 'customer_segment'])
    upload_to_s3(df, 'account_master', 'account_master_extract.csv')

def task_sales_orders():
    products_data = [
        ["P-1001", "Sales Cloud - Enterprise", "Sales Cloud", 15000],
        ["P-1002", "Sales Cloud - Pro", "Sales Cloud", 8000],
        ["S-2001", "Service Cloud - Unlimited", "Service Cloud", 18000],
        ["D-3001", "Data Cloud - Base", "Data Cloud", 25000],
        ["T-4001", "Tableau Creator", "Tableau", 840]
    ]
    df_products = pd.DataFrame(products_data, columns=['product_id', 'product_name', 'product_family', 'list_price'])
    
    industries = ["Technology", "Finance", "Retail", "Healthcare", "Manufacturing", "Energy"]
    segments = ["Enterprise", "SMB", "Mid-Market"]
    accounts_data = []
    for i in range(1, 21):
        acc_id = f"A-{i:03d}"
        acc_name = f"Company {chr(64 + (i % 26 if i % 26 != 0 else 26))} {i}"
        industry = random.choice(industries)
        segment = random.choice(segments)
        accounts_data.append([acc_id, acc_name, industry, segment])
    df_accounts = pd.DataFrame(accounts_data, columns=['account_id', 'account_name', 'industry', 'customer_segment'])
    
    reps_data = [
        ["SR-10", "Ana Flores", "Carlos Rey", "LATAM", "2022-03-01"],
        ["SR-11", "Bruno Costa", "Carlos Rey", "LATAM", "2023-05-15"],
        ["SR-12", "Dana White", "Julia Chen", "NORAM", "2021-11-01"],
        ["SR-13", "Evan Grant", "Julia Chen", "NORAM", "2022-08-20"],
        ["SR-14", "Fiona Lee", "Raj Patel", "EMEA", "2023-01-10"]
    ]
    df_reps = pd.DataFrame(reps_data, columns=['sales_rep_id', 'rep_name', 'manager_name', 'territory', 'hire_date'])
    
    orders_data = []
    product_ids = df_products['product_id'].tolist()
    product_prices = dict(zip(df_products['product_id'], df_products['list_price']))
    account_ids = df_accounts['account_id'].tolist()
    rep_ids = df_reps['sales_rep_id'].tolist()

    start_date = datetime(2023, 1, 1)

    # Generate unique random IDs for orders and opportunities
    order_ids = random.sample(range(1000000, 9999999), 101)
    opp_ids = random.sample(range(1000000, 9999999), 101)

    for i in range(1, 101):
        order_id = f"ORD-{order_ids[i-1]}"
        opp_id = f"OPP-{opp_ids[i-1]}"
        acc_id = random.choice(account_ids)
        rep_id = random.choice(rep_ids)
        
        random_days_start = random.randint(0, 500)
        opty_open = start_date + timedelta(days=random_days_start)
        random_days_close = random.randint(30, 150)
        close_date = opty_open + timedelta(days=random_days_close)
        
        prod_id = random.choice(product_ids)
        quantity = random.randint(1, 15)
        list_p = product_prices[prod_id]
        
        unit_price = int(list_p * random.uniform(0.85, 1.05))
        line_item_discount = int(unit_price * quantity * random.uniform(0.05, 0.25))
        
        orders_data.append([
            order_id, opp_id, acc_id, rep_id, 
            opty_open.strftime('%Y-%m-%d'), 
            close_date.strftime('%Y-%m-%d'),
            prod_id, quantity, unit_price, line_item_discount
        ])

    df_orders = pd.DataFrame(orders_data, columns=[
        'order_id', 'opportunity_id', 'account_id', 'sales_rep_id', 
        'opty_open_date', 'close_date', 'product_id', 'quantity', 
        'unit_price', 'line_item_discount'
    ])
    
    creation_date = datetime.now().strftime('%Y%m%d')
    filename = f'sales_orders_extract_{creation_date}.csv'
    df_orders.to_csv(filename, index=False)
    upload_to_s3(df_orders, 'sales_orders', f'sales_orders_extract_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

with DAG(
    dag_id= owner_process.upper() + '_' + 'salesforce_data_generator',
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 5,8 * * *',
    catchup=False,
    template_searchpath=[DAG_PATH],
    tags = [process_name, owner_process]
) as dag:

    t1 = PythonOperator(task_id='gen_product', python_callable=task_product_catalog)
    t2 = PythonOperator(task_id='gen_saler', python_callable=task_sales_rep_master)
    t3 = PythonOperator(task_id='gen_account', python_callable=task_account_master)
    t4 = PythonOperator(task_id='gen_orders', python_callable=task_sales_orders)
    t5 = SnowflakeOperator(
        task_id='execute_sql_snowflake',
        snowflake_conn_id='snowflake_connect',
        sql=sql_path,
        dag=dag
    )
    t6 = trigger_transformation = TriggerDagRunOperator(
    task_id="trigger_dag_model",
    trigger_dag_id=owner_process.upper() + '_' + 'salesforce_data_modeling', 
    wait_for_completion=False, 
    poke_interval=60,
    dag=dag,
    )

    [t1, t2, t3] >> t4 >> t5 >> t6