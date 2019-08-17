from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

from helpers.create_tables import drop_table_if_exists, create_statements

class CreateTableOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table_name,
                 redshift_conn_id='amazon-redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.create_table_statement = create_table_statement
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        try:
            redshift_hook = PostgresHook(self.redshift_conn_id)
            
            drop_table_command = drop_table_if_exists(self.table_name)

            logging.info(f'CreateTableOperator::Running drop table if exists command::{drop_table_command}')
            redshift_hook.run(drop_table_command)
            logging.info('CreateTableOperator::Ran drop table if exists command.')

            logging.info(f'CreateTableOperator::Running create table command::{create_statements[self.table_name]}')
            redshift_hook.run(sql_load_statement)
            logging.info(f'CreateTableOperator::Ran create table command.')

        except Exception as e:
            raise e
