from typing import Optional, Dict, Any

from airflow import DAG

from operators import StageToRedshiftOperator, DataQualityOperator


def stage_s3_to_redshift_dag(
        parent_dag_name: str,
        task_id: str,
        redshift_conn_id: str = "",
        aws_credentials_id: str = "",
        target_table: str = "",
        sql: str = "",
        s3_bucket: str = None,
        s3_key: str = None,
        json_path: Optional[str] = None,
        ignore_headers: Optional[int] = None,
        delimiter: Optional[str] = None,
        default_args: Dict[str, Any] = {},
        *args,
        **kwargs,
):
    dag = DAG(
        dag_id=f"{parent_dag_name}.{task_id}",
        default_args=default_args,
        **kwargs
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id=f"{parent_dag_name}.Stage_events",
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        target_table=target_table,
        sql=sql,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        json_path=json_path,
        dag=dag,
    )

    check_data_task = DataQualityOperator(
        task_id=f"{parent_dag_name}.Verify_Has_Rows",
        conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table=target_table,
        dag=dag,
    )

    stage_events_to_redshift >> check_data_task

    return dag