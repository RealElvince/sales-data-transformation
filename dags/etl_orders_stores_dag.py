from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os
from airflow.utils.dates import days_ago

# define dag path
dag_path = os.getcwd()

# function to join orders and salespersons and store it in a database
def join_orders_salespersons():
    orders = pd.read_csv(f"{dag_path}/data/raw/orders.csv")
    salespersons = pd.read_csv(f"{dag_path}/data/raw/salespersons.csv")
    
    joined_orders_salespersons = pd.merge(orders,salespersons,left_on="salesperson_id", right_on="id", how="inner")
    
    conn = sqlite3.connect(f"{dag_path}/db/orders.db")
    joined_orders_salespersons.to_sql('orders_salespersons', conn, if_exists='replace', index=False)
    conn.close()




# function to join salespersons and stores and store it in a database

def join_salespersons_stores():
    salespersons = pd.read_csv(f"{dag_path}/data/raw/salespersons.csv")
    stores = pd.read_csv(f"{dag_path}/data/raw/stores.csv")
    
    joined_salespersons_stores = pd.merge(salespersons,stores,left_on="id", right_on="salesperson_id", how="inner")
    
    conn = sqlite3.connect(f"{dag_path}/db/stores.db")
    joined_salespersons_stores.to_sql('salespersons_stores', conn, if_exists='replace', index=False)
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
    'etl_orders_stores_dag',
    default_args=default_args,
    description='ETL DAG for orders and stores data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# define tasks
join_orders_salespersons_task = PythonOperator(
    task_id='join_orders_salespersons',
    python_callable=join_orders_salespersons,
    dag=dag,
)

join_salespersons_stores_task = PythonOperator(
    task_id='join_salespersons_stores',
    python_callable=join_salespersons_stores,
    dag=dag,
)

# set task dependencies
join_orders_salespersons_task >> join_salespersons_stores_task