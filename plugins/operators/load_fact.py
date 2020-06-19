from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Append only
    """

    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {} ({})
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 final_table: str,
                 dql_sql: str,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.dql_sql = dql_sql
        self.target_table = final_table
        self.redshitft_conn = PostgresHook(postgres_conn_id=redshift_conn_id)

    def execute(self, context):
        self.log.info(f'Running {self.__class__.__name__}')

        formatted_sql = self.insert_sql.format(self.target_table, self.dql_sql)

        self.log.info(f"Loading fact table '{self.target_table}'")
        self.redshitft_conn.run(formatted_sql)
