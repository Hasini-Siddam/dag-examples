from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


args = {
    'owner' : 'airflow',
    'retries':5,
    'retry_delay' : timedelta(minutes=5)
}

def greet():
    print("Good Morning!")


with DAG(
    default_args=args,
    dag_id='test_python_operator',
    description="first dag using python operator",
    start_date = datetime(2022, 9, 14),
    schedule_interval='@daily'
) as dag:
    task1= PythonOperator(task_id='greet',python_callable=greet)

    task1
