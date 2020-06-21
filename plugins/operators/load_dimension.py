from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Truncate-insert pattern
    """

    ui_color = '#80BD9E'

    default_schema = "public"

    inser_sql = """
        INSERT INTO {}.{} ({})
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 final_table: str,
                 dql_sql: str,
                 should_truncate_before_insert: bool = True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.should_truncate_before_insert = should_truncate_before_insert
        self.dql_sql = dql_sql
        self.final_table = final_table

        self.redshift_conn = PostgresHook(postgres_conn_id=redshift_conn_id)

    def execute(self, context):
        self.log.info(f'Running {self.__class__.__name__}')

        formatted_sql = self.inser_sql.format(self.default_schema,
                                              self.final_table, self.dql_sql)

        if self.should_truncate_before_insert:
            self.log.info(f"Truncating dimension table '{self.final_table}'")
            self.redshift_conn.run(f"TRUNCATE TABLE {self.final_table}")

        self.log.info(f"Loading dimension table '{self.final_table}'")
        self.redshift_conn.run(formatted_sql)
