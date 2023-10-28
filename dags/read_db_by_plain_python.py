from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import mysql.connector

def reading_data():
    mydb = mysql.connector.connect(
        host="mydb",
        user="admin",
        password="secret",
        database="homestead"
    )
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM customers")
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
}

with DAG(
    'data_reading_dag',
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
    
    reading_data_task = PythonOperator(
        task_id='reading_data',
        python_callable=reading_data
    )

start >> reading_data_task >> end
