from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 aws_credentials_id: str,
                 table: str,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table

        self.aws_credentials = AwsHook(aws_conn_id=aws_credentials_id).get_credentials()
        self.redshift = PostgresHook(postgres_conn_id=conn_id)

    def execute(self, context):
        self.log.info(f"Running {self.__class__.__name__}")

        formatted_dql = f"SELECT COUNT(*) FROM {self.table}"

        self.log.info(f"Executing data quality check on table '{self.table}'")
        result = self.redshift.get_records(formatted_dql)

        if len(result) < 1 or len(result[0]) < 1:
            raise ValueError(f"Select statement on table '{self.table}' retrieved no results...")

        num_records = result[0][0]

        if num_records < 1:
            raise ValueError(f"Table '{self.table}' is empty...")
