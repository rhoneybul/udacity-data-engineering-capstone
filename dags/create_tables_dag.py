from datetime import datetime, timedelta
import os 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from operators.create_tables import CreateTableOperator
from operators.etl import ETLOperator
from operators.create_dimensions import DimensionTableOperator
from operators.helpers.read_dataframes import read_immigration_data, read_global_temperatures, read_global_temperatures_by_country, read_demographics, read_airport_codes
from operators.helpers.clean_dfs import clean_immigration_data, clean_global_temperatures, clean_global_temperatures_by_country, clean_demographics, clean_airport_codes

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

start_operator = PythonOperator(task_id='begin_execution',
                                python_callable=begin_execution,
                                dag=dag)

finish_operator = PythonOperator(task_id='finish_execution',
                                 python_callable=finish_execution,
                                 dag=dag)

etl_immigration_data = ETLOperator(task_id='etl_immigration_data',
                                   read_df=read_immigration_data,
                                   clean_df=clean_immigration_data,
                                   table_name='immigrations',
                                   dag=dag)                                 

etl_global_temperatures = ETLOperator(task_id='global_temperatures_data',
                                      read_df=read_global_temperatures,
                                      clean_df=clean_global_temperatures,
                                      table_name='global_temperatures',
                                      dag=dag)
                                
etl_global_temperatures_by_country = ETLOperator(task_id='global_temperatures_by_country_data',
                                                 read_df=read_global_temperatures_by_country,
                                                 clean_df=clean_global_temperatures_by_country,
                                                 table_name='global_temperatures_by_country',
                                                 dag=dag)
                                                
etl_demographics = ETLOperator(task_id='demographics_data',
                               read_df=read_demographics,
                               clean_df=clean_demographics,
                               table_name='demographics',
                               dag=dag)
                    
etl_airport_codes = ETLOperator(task_id='airport_codes_data',
                                read_df=read_aiport_codes,
                                clean_df=clean_aiport_codes,
                                table_name='airport_codes',
                                dag=dag)


create_countries_dimension_table = DimensionTableOperator(task_id='create_countries_dimensions',
                                                          sql_statement='select count(*) from global_temperatures',
                                                          dag=dag)
                                                    
create_cities_dimension_table = DimensionTableOperator(task_id='create_cities_dimensions',
                                                       sql_statement='select count(*) from global_temperatures',
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

start_operator >> etl_immigration_data 
start_operator >> etl_global_temperatures
start_operator >> etl_global_temperatures_by_country
start_operator >> etl_demographics
start_operator >> etl_airport_codes

etl_global_temperatures >> create_countries_dimension_table
etl_global_temperatures_by_country >> create_countries_dimension_table

etl_demographics >> create_cities


# start_operator >> create_immigration >> finish_operator
# start_operator >> global_temperatures >> finish_operator
# start_operator >> global_temperatures_by_country >> finish_operator
# start_operator >> countries >> finish_operator
# start_operator >> demographics >> finish_operator
# start_operator >> cities >> finish_operator
# start_operator >> airport_codes >> finish_operator