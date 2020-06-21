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
        SECRET_ACCESS_KEY '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 target_table: str = "",
                 sql: str = "",
                 s3_bucket: Optional[str] = None,
                 s3_key: Optional[str] = None,
                 json_path: Optional[str] = None,
                 ignore_headers: int = 1,
                 delimiter: str = ',',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.sql = sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter

    def execute(self, context):
        print(context)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Cleaning table '{self.target_table}' before loading...")
        # redshift.run(self.sql) # TODO: Remove this
        redshift.run(f"TRUNCATE TABLE {self.target_table}")

        s3_file_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        s3_json_path = f"s3://{self.s3_bucket}/{self.json_path}"

        formatted_s3_copy_command = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_file_path,
            credentials.access_key,
            credentials.secret_key,
        )

        if self.s3_key.endswith(".json"):
            formatted_s3_copy_command += """
                IGNOREHEADER {}
                DELIMITER '{}'
            """.format(self.ignore_headers, self.delimiter, )
        else:
            if self.json_path is None:
                formatted_s3_copy_command += " json 'auto'"
            else:
                formatted_s3_copy_command += """
                    json '{}'
                """.format(s3_json_path)

        self.log.info(f"Copying data from s3 '{s3_file_path}' to redshift "
                      f"final table '{self.target_table}'...")
        redshift.run(formatted_s3_copy_command)
