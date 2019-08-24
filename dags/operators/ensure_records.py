from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

from operators.helpers.create_table_statements import drop_table_if_exists, create_statements

class EnsureRecords(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table_name,
                 redshift_conn_id='amazon-redshift',
                 *args, **kwargs):

        super(EnsureRecords, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        try:
            conn = PostgresHook(self.redshift_conn_id).get_conn()

            logging.info(f"Ensuring Valid Records::{self.table_name}")

            cursor = conn.cursor()

            cursor.execute(f'select count(*) from {self.table_name}')

            total_count = cursor.fetchone()

            logging.info(f"Total Count::{total_count}")

            if total_count == 0:
                raise Exception(f"There are no records for table::{self.table_name}")

            conn.commit()

            cursor.close()

            conn.close()

        except Exception as e:
            raise e
