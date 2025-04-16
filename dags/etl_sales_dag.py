from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from datetime import datetime
from airflow.utils.dates import days_ago
import os

# define dag path
dag_path = os.getcwd()
print(dag_path)

# total sales per row transformation function
def total_sales_transformation():
   total_sales = pd.read_csv(f"{dag_path}/data/raw/sales.csv")
   total_sales['date'] = pd.to_datetime(total_sales['date'])
   total_sales['total_sales'] = total_sales['quantity'] * total_sales['price']
   total_sales.to_csv(f"{dag_path}/data/transformed/sales_transformed.csv", index=False)

# join sales and customer transformation function
def join_sales_customer_transformation():
    sales = pd.read_csv(f"{dag_path}/data/raw/sales.csv")
    customer = pd.read_csv(f"{dag_path}/data/raw/customer.csv")
    sales_customer = pd.merge(sales, customer, on='customer_id', how='inner')
    sales_customer.to_csv(f"{dag_path}/data/transformed/sales_customer_transformed.csv", index=False)   
    
# load and save to database function
def load_sales():
   conn = sqlite3.connect("/usr/local/airflow/db/salesanalytics.db")
   total_sales_data = pd.read_csv(f"{dag_path}/data/transformed/sales_transformed.csv")
   total_sales_data.to_sql('total_sales', conn, if_exists='replace', index=False)
   
   customers_sales_data = pd.read_csv(f"{dag_path}/data/transformed/sales_customer_transformed.csv")
   customers_sales_data.to_sql('customers_sales', conn, if_exists='replace', index=False)
   conn.close()
   
# define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define dag
dag = DAG(
    'etl_sales_dag',
    default_args=default_args,
    description='ETL DAG for sales data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# define tasks
transform_sales= PythonOperator(
    task_id='total_sales_transformation',
    python_callable=total_sales_transformation,
    dag=dag,
)


join_sales_customers = PythonOperator(
    
    task_id='join_sales_customer_transformation',
    python_callable=join_sales_customer_transformation,
    dag=dag,
)

load_sales_data = PythonOperator(
    task_id='load_sales',
    python_callable=load_sales,
    dag=dag,
)

# set task dependencies

transform_sales >> join_sales_customers >> load_sales_data