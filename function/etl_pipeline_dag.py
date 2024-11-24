from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from etl_pipeline import extract_data, transform_data, load_data_to_db

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# DAG Defined
with DAG('etl_pipeline_dag', default_args=default_args, schedule_interval='@daily') as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        op_kwargs={'file_path': 'etl_data/branch_sales.csv'}
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        op_kwargs={'branch_sales': branch_sales, 'online_sales': online_sales}
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data_to_db,
        op_kwargs={'data': branch_sales, 'table_name': 'branch_sales', 'db_uri': DB_URI}
    )

    extract_task >> transform_task >> load_task
