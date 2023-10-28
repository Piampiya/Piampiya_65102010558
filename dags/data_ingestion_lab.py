from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import csv
import pandas as pd
from datetime import datetime
import csv
import pandas as pd
from datetime import datetime
from airflow.hooks.mysql_hook import MySqlHook

def ingest_data():
    with open('/tmp/customer.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name"])
        request = "SELECT * FROM customers"
        mysql_hook = MySqlHook(mysql_conn_id='mydb', schema="homestead")
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        for (id_value, name_value) in cursor:
            writer.writerow([id_value, name_value])

def transform_data():
    df = pd.read_csv('/tmp/customer.csv')
    convert_dict = {'id': int, 'name': str}
    df = df.astype(convert_dict)
    df['last_update'] = datetime.now()
    df.to_parquet('/tmp/customer_temp.parquet', engine='fastparquet')

def upload_data():
    df = pd.read_parquet('/tmp/customer_temp.parquet')
    delete_sql = "DELETE FROM customers_analytics"
    mysql_hook = MySqlHook(mysql_conn_id='mydb', schema='homestead')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(delete_sql)
    sql = "INSERT INTO customers_analytics (id, name, last_update) VALUES (%s, %s, %s)"
    for i, row in df.iterrows():
        cursor.execute(sql, tuple(row))
    connection.commit()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
}

with DAG(
    'example_pipeline',
    default_args=default_args,
    schedule_interval="* * * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=['airflow_tab']
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    read_db_task = PythonOperator(
        task_id='read_db',
        python_callable=ingest_data
    )

    prepare_data_task = PythonOperator(
        task_id='prepare_data',
        python_callable=transform_data
    )

    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=upload_data
    )

    start >> read_db_task >> prepare_data_task >> insert_data_task >> end
