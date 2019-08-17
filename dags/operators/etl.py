from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging 

class ETLOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 read_df
                 table_name,
                 redshift_conn_id='amazon-redshift',
                 *args, **kwargs):
        """
        parameters:
        read_df: function to read in the data frame.
        table_name: table to insert the data into.
        """

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        try:
            df = read_df()
        except Exception as e:
            raise e
