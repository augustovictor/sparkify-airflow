from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from operators.data_quality_validation import DataQualityValidator


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 data_quality_validations: List[DataQualityValidator],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.data_quality_validations = data_quality_validations
        self.redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

    def execute(self, context):
        self.log.info(f"Running {self.__class__.__name__}")

        for validation in self.data_quality_validations:
            self.log.info(
                f"Executing data quality with statement: '{validation.sql_statement}'")

            response = self.redshift.get_records(validation.sql_statement)

            if len(response) < 1 or len(response[0]) < 1:
                error_message = f"Select statement '{validation.sql_statement}' got empty response..."
                self.log.error(error_message)
                raise ValueError(error_message)

            actual_result = response[0][0]

            if not validation.is_valid_with(actual_result=actual_result):
                error_message = f"Statement '{validation.sql_statement}' response '{actual_result}' does not match expected result '{validation.should_assert_for_equality} {validation.result_to_assert}'..."
                self.log.error(error_message)
                raise ValueError(error_message)
