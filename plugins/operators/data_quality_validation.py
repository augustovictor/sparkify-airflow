from typing import Any


class DataQualityValidator:
    def __init__(self,
                 sql_statement: str,
                 result_to_assert: Any,
                 should_assert_for_equality: bool = True
                 ):
        self.should_assert_for_equality = should_assert_for_equality
        self.sql_statement = sql_statement
        self.result_to_assert = result_to_assert

    def is_valid_with(self, actual_result: Any) -> bool:
        if self.should_assert_for_equality:
            return actual_result == self.result_to_assert

        return actual_result != self.result_to_assert
