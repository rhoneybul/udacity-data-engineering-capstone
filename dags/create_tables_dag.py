from datetime import datetime, timedelta
import os 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from operators.create_tables import CreateTableOperator

import logging

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'rhoneybul',
    'start_date': datetime(2019,1,12),
    'depends_on_past': False, 
    'retries': 0,
    'catchup_by_default': False,
    'catchup': False,
    'backfill': False,
    'email_on_retry': False,
}

dag = DAG('immigration_etl_pipeline',
          default_args=default_args,
          description='create tables in redshift.',
          schedule_interval=None,
      )

def begin_execution():
    logging.info("Create Table Execution Starting.")

def finish_execution():
    logging.info("Create Table Execution Completed.")

def read_parquet():
    data_path = '/usr/local/data/immigration-data'
    parquet_files = os.listdir(data_path)

    file_paths = [os.path.join(data_path, f) for f in parquet_files]

    logging.info(f"ParquetFiles::Parquet file paths::{file_paths}")

    for fp in file_paths:
        df = pd.read_parquet(fp)
        logging.info(f'ParquetFiles::Read parquet file')
        logging.info(df.head())

start_operator = PythonOperator(task_id='begin_execution',
                                python_callable=begin_execution,
                                dag=dag)

finish_operator = PythonOperator(task_id='finish_execution',
                                 python_callable=finish_execution,
                                 dag=dag)

# create_immigration = CreateTableOperator(task_id='create_immigration_table',
#                                          table_name='immigrations',
#                                          dag=dag)

# global_temperatures = CreateTableOperator(task_id='create_global_temperatures',
#                                           table_name='global_temperatures',
#                                           dag=dag)

# global_temperatures_by_country = CreateTableOperator(task_id='create_global_temperatures_by_country',
#                                                      table_name='global_temperatures_by_country',
#                                                      dag=dag)                                                                                 

# countries = CreateTableOperator(task_id='create_countries',
#                                 table_name='countries',
#                                 dag=dag)

# demographics = CreateTableOperator(task_id='create_demographics',
#                                    table_name='demographics',
#                                    dag=dag)

# cities = CreateTableOperator(task_id='create_cities',
#                              table_name='cities',
#                              dag=dag)                                                                                                                        

# airport_codes = CreateTableOperator(task_id='create_airport_codes',
#                                     table_name='airport_codes',
#                                     dag=dag)                             



list_data_dir = PythonOperator(task_id='list-data-dir', python_callable=read_parquet, dag=dag)

start_operator >> list_data_dir

# start_operator >> create_immigration >> finish_operator
# start_operator >> global_temperatures >> finish_operator
# start_operator >> global_temperatures_by_country >> finish_operator
# start_operator >> countries >> finish_operator
# start_operator >> demographics >> finish_operator
# start_operator >> cities >> finish_operator
# start_operator >> airport_codes >> finish_operator