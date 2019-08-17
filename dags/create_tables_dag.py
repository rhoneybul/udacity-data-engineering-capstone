from datetime import datetime, timedelta
import os 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'rhoneybul',
    'start_date': datetime(2019,1,12),
    'depends_on_past': False, 
    'retries': 3,
    'retry_delta': timedelta(minutes=5),
    'catchup_by_default': False,
    'catchup': False,
    'backfill': False,
    'email_on_retry': False,
}

dag = DAG('create_Table_pipeline',
          default_args=default_args,
          description='create tables in redshift.',
          schedule_interval=None,
      )

def begin_execution():
    logging.info("Create Table Execution Starting.")

def finish_execution():
    logging.info("Create Table Execution Completed.")

start_operator = PythonOperator(task_id='begin_execution',
                                python_callable=begin_execution,
                                dag=dag)

finish_operator = PythonOperator(task_id='finish_execution',
                                 python_callable=finish_execution,
                                 dag=dag)

start_operator >> finish_operator