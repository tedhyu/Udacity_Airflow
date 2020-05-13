from datetime import datetime, timedelta
import logging
#airflow list_dags
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (PostgresOperator, PythonOperator,
                                StageToRedshiftOperator, S3ToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



#
# TODO: Replace the data quality checks with the HasRowsOperator
#


dag = DAG('udac_example_dag',
          description='Load and transform data in Redshift with Airflow',
          start_date = datetime.now()

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
    s3_key="song_data/A/W/W"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data"
)

load_songplays_table = LoadFactOperator(
    task_id="load_songplays_table",
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert
)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table