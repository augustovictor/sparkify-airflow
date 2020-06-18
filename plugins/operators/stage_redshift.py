from typing import Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.s3_to_redshift_operator import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Docs
    """

    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 target_table: str = "",
                 sql: str = "",
                 s3_bucket: Optional[str] = None,
                 s3_key: Optional[str] = None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.sql = sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        print(aws_hook)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Cleanning table before insert...")
        redshift.run(self.sql)
