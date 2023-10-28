from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np

def change_datatype():
    df = pd.read_csv('/my_tmp/tested.csv')
    convert_dict = {'Pclass': float}
    df = df.astype(convert_dict)
    
    # Save the DataFrame as a Parquet file
    # df.to_parquet('/my_tmp/tested_2.parquet', engine='fastparquet')
    df.to_csv('/my_tmp/tested_2.csv')
def save_data():
    # Read the Parquet file saved in the change_datatype function
    df = pd.read_csv('/my_tmp/tested_2.csv')
    
    # Filter rows where 'Sex' is 'male'
    male_rows = df[df['Sex'] == 'male']
    
    # Save the filtered DataFrame as a Parquet file
    male_rows.to_parquet('/my_tmp/titanic_lab.parquet', engine='fastparquet')

def remove_columns():
    df = pd.read_csv('/my_tmp/tested.csv')
    df = df.rename(columns={'Sex': 'Gender'})

    df.to_csv('/my_tmp/removed.csv')  

def rename_columns_Sex():

    df = pd.read_csv('/my_tmp/removed.csv')

    df = df.rename(columns={'Sex': 'Gender'})
    
    df.to_parquet('/my_tmp/renamed.parquet', engine='fastparquet')

def filter_age():
    df = pd.read_csv('/my_tmp/tested.csv')
    filter_age = df[df['Age'] > 25]

    filter_age.to_csv('/my_tmp/filter_age.csv')

def random_Price():
    df = pd.read_csv('/my_tmp/filter_age.csv')
    df['Price'] = np.random.randint(low=1, high=100, size=len(df))

    df.to_parquet('/my_tmp/random_Price.parquet', engine='fastparquet')
    df.to_csv('/my_tmp/random_Price.csv')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
}

with DAG(
    'dataframe_lab1',
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
    change_datatype_task = PythonOperator(
        task_id='change_datatype',
        python_callable=change_datatype
    )
    save_data_task = PythonOperator(
        task_id='save_data',
        python_callable=save_data
    )
    remove_columns_task = PythonOperator(
        task_id='remove_columns',
        python_callable=remove_columns
    )
    rename_columns_Sex_task = PythonOperator(
        task_id='rename_columns_Sex',
        python_callable=rename_columns_Sex
    )
    filter_age_task = PythonOperator(
        task_id='filter_age',
        python_callable=filter_age
    ) 
    random_Price_task = PythonOperator(
        task_id='random_Price',
        python_callable=random_Price
    ) 
    
    start >>  remove_columns_task >> rename_columns_Sex_task >> end
