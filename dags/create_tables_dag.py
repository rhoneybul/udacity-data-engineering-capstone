from datetime import datetime, timedelta
import os 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from operators.create_tables import CreateTableOperator
from operators.ensure_records import EnsureRecords
from operators.ensure_distinct import EnsureDistinctRecords
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

etl_immigration_data = ETLOperator(task_id='etl_immigration_data',
                                   read_df=read_immigration_data,
                                   clean_df=clean_immigration_data,
                                   table_name='immigrations',
                                   write_subset=False,
                                   write_sample=1000,
                                   dag=dag)                                 

etl_global_temperatures = ETLOperator(task_id='global_temperatures_data',
                                      read_df=read_global_temperatures,
                                      clean_df=clean_global_temperatures,
                                      table_name='global_temperatures',
                                      write_subset=False,
                                      write_sample=1000,
                                      dag=dag)
                                
etl_global_temperatures_by_country = ETLOperator(task_id='global_temperatures_by_country_data',
                                                 read_df=read_global_temperatures_by_country,
                                                 clean_df=clean_global_temperatures_by_country,
                                                 table_name='global_temperatures_by_country',
                                                 write_subset=False,
                                                 write_sample=1000,
                                                 dag=dag)
                                                
etl_demographics = ETLOperator(task_id='demographics_data',
                               read_df=read_demographics,
                               clean_df=clean_demographics,
                               table_name='demographics',
                               write_subset=False,
                               write_sample=1000,
                               dag=dag)
                    
etl_airport_codes = ETLOperator(task_id='airport_codes_data',
                                read_df=read_airport_codes,
                                clean_df=clean_airport_codes,
                                table_name='airport_codes',
                                write_subset=False,
                                write_sample=1000,
                                dag=dag)

create_countries_sql = """
insert into countries (name)
select distinct country from global_temperatures_by_country;
"""

create_countries_dimension_table = DimensionTableOperator(task_id='create_countries_dimensions',
                                                          sql_statement=create_countries_sql,
                                                          dag=dag)
                                                    
create_cities_sql = """
insert into cities (state_code, state, name)
select distinct state_code, state, city from demographics;
"""                                                    

create_cities_dimension_table = DimensionTableOperator(task_id='create_cities_dimensions',
                                                       sql_statement=create_cities_sql,
                                                       dag=dag)

create_immigration = CreateTableOperator(task_id='create_immigration_table',
                                         table_name='immigrations',
                                         dag=dag)

create_global_temperatures = CreateTableOperator(task_id='create_global_temperatures',
                                                 table_name='global_temperatures',
                                                 dag=dag)

create_global_temperatures_by_country = CreateTableOperator(task_id='create_global_temperatures_by_country',
                                                            table_name='global_temperatures_by_country',
                                                            dag=dag)                                                                                 

create_countries = CreateTableOperator(task_id='create_countries',
                                       table_name='countries',
                                       dag=dag)

create_demographics = CreateTableOperator(task_id='create_demographics',
                                          table_name='demographics',
                                          dag=dag)

create_cities = CreateTableOperator(task_id='create_cities',
                                    table_name='cities',
                                    dag=dag)                                                                                                                        

create_airport_codes = CreateTableOperator(task_id='create_airport_codes',
                                           table_name='airport_codes',
                                           dag=dag)           

check_immigration_records = EnsureRecords(task_id='ensure_immigration_recoreds',
                                          table_name='immigrations',
                                          dag=dag)

check_global_temperature_records = EnsureRecords(task_id='ensure_global_temperature_records',
                                                 table_name='global_temperatures',
                                                 dag=dag)

check_global_temperature_by_country_records = EnsureRecords(task_id='ensure_global_temperature_by_country_records',
                                                            table_name='global_temperature_by_country',
                                                            dag=dag)

check_country_records = EnsureRecords(task_id='ensure_country_records',
                                      table_name='countries',
                                      dag=dag)

check_demographic_records = EnsureRecords(task_id='check_demographic_records',
                                          table_name='demographics',
                                          dag=dag)

check_cities = EnsureRecords(task_id='check_cities_records',
                             table_name='cities',
                             dag=dag)

check_airport_codes = EnsureRecords(task_id='check_aiport_codes',
                                    table_name='airport_codes',
                                    dag=dag)

check_immigration_distinct_records = EnsureDistinctRecords(task_id='check_distinct_immigration_records',
                                                           table_name='immigrations',
                                                           distinct_column='ins_number',
                                                           dag=dag)

check_cities_distinct_records = EnsureDistinctRecords(task_id='check_distinct_city_records',
                                                      table_name='cities',
                                                      distinct_column='name',
                                                      dag=dag)     

check_countries_distinct_records = EnsureDistinctRecords(task_id='check_distinct_country_records',
                                                         table_name='countries',
                                                         distinct_column='name',
                                                         dag=dag)

check_airport_codes_distinct_records = EnsureDistinctRecords(task_id='check_airport_codes_distinct_records',
                                                             table_name='airport_codes',
                                                             distinct_column='id',
                                                             dag=dag)                                                                                                                                                                                                                                                                                                                                                                                                                                                                             

start_operator >> create_immigration >> etl_immigration_data >> check_immigration_records >> check_immigration_distinct_records
start_operator >> create_global_temperatures >> etl_global_temperatures >> check_global_temperature_records
start_operator >> create_global_temperatures_by_country >> etl_global_temperatures_by_country >> check_global_temperature_by_country_records
start_operator >> create_demographics >> etl_demographics >> check_demographic_records
start_operator >> create_airport_codes >> etl_airport_codes >> check_airport_codes >> check_airport_codes_distinct_records
start_operator >> create_countries >> create_countries_dimension_table >> check_country_records >> check_countries_distinct_records
start_operator >> create_cities >> create_cities_dimension_table >> check_cities >> check_cities_distinct_records


etl_global_temperatures >> create_countries_dimension_table
etl_global_temperatures_by_country >> create_countries_dimension_table

etl_demographics >> create_cities_dimension_table

## dimension table operators

## data quality checks