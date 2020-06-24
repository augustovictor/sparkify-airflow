from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DdlRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id: str,
            ddl_sql: str,
            *args,
            **kwargs
    ):
        super(DdlRedshiftOperator, self).__init__(*args, **kwargs)
        self.sql = ddl_sql

        self.conn = PostgresHook(postgres_conn_id=redshift_conn_id)

    def execute(self, context):
        self.log.info(f"Running {self.__class__.__name__}")
        self.log.info(context)

        self.log.info(f"Executing DDL statements...")
        self.conn.run(self.sql)
