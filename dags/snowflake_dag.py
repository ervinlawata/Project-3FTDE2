from datetime import datetime, timedelta
from airflow import DAG
import os
import sys
from airflow.operators.python_operator import PythonOperator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dag_file_path = os.path.dirname(os.path.abspath(__file__))

from etls.snowflake_etl import  load_csv_to_snowflake_staging,load_to_snowflake,tf_dm_top_category_sales,tf_dm_top_employee_revenue,tf_dm_supplier_revenue
from etls.postgres_etl import  load_to_postgres,extract_csv

default_args = {
    'owner': 'Query-Query Ninja',
    'start_date': datetime(2024, 6, 18)
}

dag = DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    schedule_interval='@daily', 
)

# Create the tasks
extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=dag,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

tf_dm_supplier_revenue = PythonOperator(
    task_id='tf_dm_supplier_revenue',
    python_callable=tf_dm_supplier_revenue,
    dag=dag,
)
tf_dm_top_category_sales = PythonOperator(
    task_id='tf_dm_top_category_sales',
    python_callable=tf_dm_top_category_sales,
    dag=dag,
)

tf_dm_top_employee_revenue = PythonOperator(
    task_id='tf_dm_top_employee_revenue',
    python_callable=tf_dm_top_employee_revenue,
    dag=dag,
)

load_csv_to_snowflake_staging = PythonOperator(
    task_id='load_csv_to_snowflake_staging',
    python_callable=load_csv_to_snowflake_staging,
    dag=dag,
)

load_to_snowflake = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag,
)


extract_csv_task >> load_to_postgres_task >> tf_dm_supplier_revenue >> tf_dm_top_category_sales >> tf_dm_top_employee_revenue >> load_csv_to_snowflake_staging >> load_to_snowflake