from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

def reading_data():
    request = "SELECT * FROM customers"
    mysql_hook = MySqlHook(mysql_conn_id = 'localdb', schema = 'homestead')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    print(sources)

default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'email': ['airflow@example.com'],
}

with DAG(
    'read_db_by_mysql_hook_1',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['airflow_tab'],
) as dag:
    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )
    
    python_task= PythonOperator(
        task_id='read_db',
        python_callable=reading_data
    )

    start >> python_task >> end