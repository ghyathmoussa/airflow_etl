from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import mysql.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 31, 10, 0, 0),
    'retries': 1
}

dag = DAG('country_dag', default_args=default_args, schedule_interval='@daily')

# main functions

def start_dag():
    print('country DAG started.........\n')

def end_dag():
    print('country DAG ended......\n')

# load country from json file
def load_country():
    with open('../names.json','r') as f:
        data = json.load(f)
    return data

def save_to_sql():
    data = load_country()
    conn = mysql.connector.connect(
        user='kartca',
        password='kartca',
        host='localhost',
        database='kartca_db'
    )
    cursor = conn.cursor()
    for country_code,country_name in data.items():
        print(country_name,country_code)
        query = f"INSERT INTO country (country_code,country_name) VALUES ('{country_code,country_name}')"
        cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

start_task = PythonOperator(
    task_id='start_dag',
    python_callable=start_dag,
    dag=dag
)

read_task = PythonOperator(
    task_id='read_json',
    python_callable=load_country,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_data',
    python_callable=save_to_sql,
    dag=dag
)

end_task = PythonOperator(
    task_id='end_dag',
    python_callable=end_dag,
    dag=dag
)

start_task >> read_task >> insert_task >> end_task