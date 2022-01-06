from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from spotify_etl import run_spotify_etl

default_args = {
    'owner': 'e-capi',
    'depends_on_past': False,
    'start_date': datetime(2020,11,8), #datetime(2022,1,5)
    'email': ['emiliocapidel@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Spotify pipeline',
    schedule_interval= timedelta(days=1),
)

def just_a_funct():
    print('something usefull ?')

#Operators determine what's done by a task ( 1 operator = 1 task)
run_etl = PythonOperator(
    task_id="whole_spotify_etl",
    python_callable=run_spotify_etl,
    dag=dag,
)

#Order of task execution
run_etl
