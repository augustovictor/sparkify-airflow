from datetime import datetime, timedelta
from os import path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from helpers import SqlQueries
from operators import (DataQualityOperator,
                       DdlRedshiftOperator, DataQualityValidation,
                       LoadFactOperator, LoadDimensionOperator)
from stage_s3_to_redshift_and_validate_subdag import stage_s3_to_redshift_dag

log = LoggingMixin().log
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
AIRFLOW_AWS_CREDENTIALS_ID = "aws_credentials"
AIRFLOW_REDSHIFT_CONN_ID = "redshift"

# S3_BUCKET="udacity-dend"
S3_BUCKET="victor-nano-sparkify-raw-data-us-west-2"
S3_LOGS_KEY="log_data"
S3_SONGS_KEY="song_data"
LOG_JSONPATH="log_json_path.json"

default_args = {
    'owner': 'Victor Costa',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 16),
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
}

main_task_id = 'sparkify_etl_dw'

dag = DAG(dag_id=main_task_id,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False,
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

sql_file_name = 'create_tables.sql'
sql_path = path.join(path.dirname(path.abspath(__file__)), sql_file_name)
target_events_table = "public.staging_events"
target_songs_table = "public.staging_songs"

sql_content = None

try:
    with open(sql_file_name) as reader:
        sql_content = reader.read()

except Exception as err:
    log.error(f"Failure when reading file {sql_path}")

db_setup_task = DdlRedshiftOperator(
    task_id="DDL_Redshift",
    redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    ddl_sql=sql_content,
    dag=dag,
)

stage_events_task_id = "Stage_Events_to_Redshift_And_Validate"
stage_events_s3_to_redshift_and_validate_task = SubDagOperator(
    task_id=("%s" % stage_events_task_id),
    dag=dag,
    subdag=stage_s3_to_redshift_dag(
        parent_dag_name=main_task_id,
        task_id=stage_events_task_id,
        redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
        aws_credentials_id=AIRFLOW_AWS_CREDENTIALS_ID,
        target_table=target_events_table,
        sql=sql_content,
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOGS_KEY,
        json_path=LOG_JSONPATH,
        default_args=default_args
    )
)

stage_songs_task_id = "Stage_Songs_to_Redshift_And_Validate"
stage_songs_s3_to_redshift_and_validate_task = SubDagOperator(
    task_id=stage_songs_task_id,
    dag=dag,
    subdag=stage_s3_to_redshift_dag(
        parent_dag_name=main_task_id,
        task_id=stage_songs_task_id,
        redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
        aws_credentials_id=AIRFLOW_AWS_CREDENTIALS_ID,
        target_table=target_songs_table,
        sql=sql_content,
        s3_bucket=S3_BUCKET,
        s3_key=S3_SONGS_KEY,
        default_args=default_args
    )
)

facts_songplays_table_name = "songplays"
load_songplays_table_task = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    final_table=facts_songplays_table_name,
    dql_sql=SqlQueries.songplay_table_insert,
    dag=dag
)

dim_users_table_name = "users"
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    final_table=dim_users_table_name,
    dql_sql=SqlQueries.user_table_insert,
    dag=dag
)

dim_songs_table_name = "songs"
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    final_table=dim_songs_table_name,
    dql_sql=SqlQueries.song_table_insert,
    dag=dag
)

dim_artists_table_name = "artists"
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    final_table=dim_artists_table_name,
    dql_sql=SqlQueries.artist_table_insert,
    dag=dag
)

dim_time_table_name = "time"
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    final_table=dim_time_table_name,
    dql_sql=SqlQueries.time_table_insert,
    dag=dag
)

validation_events = DataQualityValidation(
    sql_statement=f"SELECT COUNT(*) FROM {target_events_table}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

validation_time = DataQualityValidation(
    sql_statement=f"SELECT COUNT(*) FROM {dim_time_table_name}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

validation_artists = DataQualityValidation(
    sql_statement=f"SELECT COUNT(*) FROM {dim_artists_table_name}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

validation_songs = DataQualityValidation(
    sql_statement=f"SELECT COUNT(*) FROM {dim_songs_table_name}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

validation_users = DataQualityValidation(
    sql_statement=f"SELECT COUNT(*) FROM {dim_users_table_name}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

validation_songplays = DataQualityValidation(
    sql_statement=f"SELECT COUNT(*) FROM {facts_songplays_table_name}",
    result_to_assert=0,
    should_assert_for_equality=False,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    data_quality_validations=[
        validation_songplays,
        validation_songs,
        validation_artists,
        validation_events,
        validation_time,
        validation_users
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG dependency setup
start_operator >> db_setup_task

db_setup_task >> stage_events_s3_to_redshift_and_validate_task
db_setup_task >> stage_songs_s3_to_redshift_and_validate_task

stage_events_s3_to_redshift_and_validate_task >> load_songplays_table_task
stage_songs_s3_to_redshift_and_validate_task >> load_songplays_table_task

load_songplays_table_task >> load_user_dimension_table
load_songplays_table_task >> load_song_dimension_table
load_songplays_table_task >> load_artist_dimension_table
load_songplays_table_task >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator









# start_operator >> stage_songs_to_redshift
#
# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table
#
# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table
#
# load_user_dimension_table >> run_quality_checks
# load_song_dimension_table >> run_quality_checks
# load_artist_dimension_table >> run_quality_checks
# load_time_dimension_table >> run_quality_checks

# run_quality_checks >> end_operator