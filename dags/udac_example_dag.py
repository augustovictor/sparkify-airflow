from datetime import datetime, timedelta
from os import path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
AIRFLOW_AWS_CREDENTIALS_ID = "aws_credentials"
AIRFLOW_REDSHIFT_CONN_ID = "redshift"

S3_BUCKET="udacity-dend"
S3_LOGS_KEY="log_data"
S3_SONGS_KEY="song_data"
LOG_JSONPATH="log_json_path.json"

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 6, 16),
    # 'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    # 'email_on_retry': False,
}

dag = DAG('etl_dw',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag, catchup=False)

sql_file_name = 'create_tables.sql'
sql_path = path.join(path.dirname(path.abspath(__file__)), sql_file_name)
target_events_table = "public.staging_events"

with open(sql_file_name) as reader:
    sql_file = reader.read()

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=AIRFLOW_REDSHIFT_CONN_ID,
        aws_credentials_id=AIRFLOW_AWS_CREDENTIALS_ID,
        target_table=target_events_table,
        sql=sql_file,
        s3_bucket=S3_BUCKET,
        s3_key=S3_LOGS_KEY,
        json_path=LOG_JSONPATH,
        dag=dag,
    )

check_data_task = DataQualityOperator(
    task_id="Verify_Has_Rows",
    conn_id=AIRFLOW_REDSHIFT_CONN_ID,
    aws_credentials_id=AIRFLOW_AWS_CREDENTIALS_ID,
    table=target_events_table,
    dag=dag,
)

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag
# )
#
# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )
#
# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )
#
# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )
#
# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )
#
# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift

stage_events_to_redshift >> check_data_task
check_data_task >> end_operator
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