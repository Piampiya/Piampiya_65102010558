import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd

def reading_data_and_saving_csv():
    request = "SELECT * FROM customers"
    mysql_hook = MySqlHook(mysql_conn_id = 'mydb', schema = 'homestead')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    
    df = pd.DataFrame(sources, columns =[desc[0] for desc in cursor.description])

    output_dir = '/data/read_db.csv'
    
    df.to_csv(output_dir,index = True)

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'email' : ['airflow@example.com'],
}

with DAG(
    'read_db_and_save_local',
    default_args = default_args,
    schedule_interval = timedelta(days=1),
    start_date = days_ago(2),
    tags = ['airflow_tab'],
) as dag:
    start = DummyOperator(
        task_id = 'start'
    )

    end = DummyOperator(
        task_id = 'end'
    )

    read_and_save_task = PythonOperator(
        task_id = 'read_data_and_save_csv',
        python_callable = reading_data_and_saving_csv,
    )

    start >> read_and_save_task >> end