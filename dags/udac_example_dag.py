from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (PostgresOperator,
                                StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.dummy_operator import DummyOperator
#
# TODO: Replace the data quality checks with the HasRowsOperator
#
default_args = {
    'owner': 'Jar Yu',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          description='Load and transform data in Redshift with Airflow',
          start_date = datetime.now(),
          schedule_interval="@hourly",
          catchup=False,

)

start_operator = PostgresOperator(
    task_id="begin_execution",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/W/X",
    extra_params = "format as json 'auto'"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data/2018/11",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'"
)

load_songplays_table = LoadFactOperator(
    task_id="load_songplays_table",
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

load_artists_table = LoadDimensionOperator(
    task_id="load_artists_table",
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert
)

load_songs_table = LoadDimensionOperator(
    task_id="load_songs_table",
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    sql=SqlQueries.song_table_insert
)

load_users_table = LoadDimensionOperator(
    task_id="load_users_table",
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    sql=SqlQueries.user_table_insert
)

load_time_table = LoadDimensionOperator(
    task_id="load_time_table",
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    sql=SqlQueries.time_table_insert
)

data_quality = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table = 'songplays'
    )

end_operator = DummyOperator(task_id='end_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_artists_table
load_songplays_table >> load_users_table
load_songplays_table >> load_songs_table
load_songplays_table >> load_time_table
load_artists_table >> data_quality
load_users_table >> data_quality
load_songs_table >> data_quality
load_time_table >> data_quality
data_quality >> end_operator