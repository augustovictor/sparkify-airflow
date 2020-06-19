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
        self.log.info('DataQualityOperator step')
        formatted_dql = f"SELECT COUNT(*) FROM {self.table}"
        result = self.redshift.get_records(formatted_dql)

        if len(result) < 1 or len(result[0]) < 1:
            raise ValueError("Actual result from validation task does not match expected result")

        num_records = result[0][0]

        if num_records < 1:
            raise ValueError(
                "Actual result from validation task does not match expected result")
