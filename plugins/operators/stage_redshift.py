from typing import Optional

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.macros import ds_format
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


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
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter
        self.s3_hook = S3Hook(aws_conn_id=aws_credentials_id)

    def execute(self, context):
        self.log.info("CONTEXT")
        self.log.info(context)
        execution_date = context.get('execution_date').strftime("%Y-%m-%d")

        year = ds_format(execution_date, '%Y-%m-%d', '%Y')
        month = ds_format(execution_date, '%Y-%m-%d', '%m')
        credentials = AwsHook(self.aws_credentials_id).get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        s3_file_path = f"s3://{self.s3_bucket}/{self.s3_key}/{year}/{month}"
        s3_json_path = f"s3://{self.s3_bucket}/{self.json_path}"

        formatted_s3_copy_command = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_file_path,
            credentials.access_key,
            credentials.secret_key,
        )

        formatted_s3_copy_command = self.get_formatted_s3_copy_command(
            formatted_s3_copy_command, s3_json_path)

        self.log.info(f"Cleaning table '{self.target_table}' before loading...")

        self.log.info(f"Copying data from s3 '{s3_file_path}' to redshift "
                      f"final table '{self.target_table}'...")

        if self.s3_hook.check_for_key(key=self.s3_key, bucket_name=self.s3_bucket):
            redshift.run(formatted_s3_copy_command)
        else:
            self.log.info(f"S3 object '{s3_file_path}' does not exist...")

    def get_formatted_s3_copy_command(self, formatted_s3_copy_command,
                                      s3_json_path):
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
        return formatted_s3_copy_command
