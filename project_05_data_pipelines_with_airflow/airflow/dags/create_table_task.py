from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import sql_statements

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ivalderrama',
    'start_date': datetime(2019, 12, 28),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

dag = DAG(
    'create_table_task',
    default_args=default_args,
    description='Drop and create tables to be loaded in redshift',
    schedule_interval='@hourly'
)

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)


create_table_artists = PostgresOperator(
    task_id='create_artists',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_ARTISTS,
    dag=dag
)


create_table_songplays = PostgresOperator(
    task_id='create_songplays',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGPLAYS,
    dag=dag
)


create_table_songs = PostgresOperator(
    task_id='create_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGS,
    dag=dag
)


create_table_staging_events = PostgresOperator(
    task_id='create_staging_events',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGING_EVENTS,
    dag=dag
)


create_table_staging_songs = PostgresOperator(
    task_id='create_staging_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGING_SONGS,
    dag=dag
)


create_table_users = PostgresOperator(
    task_id='create_users',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_USERS,
    dag=dag
)


create_table_time = PostgresOperator(
    task_id='create_time',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_TIME,
    dag=dag
)


end_operator = DummyOperator(task_id='stop_execution',  dag=dag)


start_operator >> create_table_artists
create_table_artists >> end_operator

start_operator >> create_table_songplays
create_table_songplays >> end_operator

start_operator >> create_table_songs
create_table_songs >> end_operator

start_operator >> create_table_staging_events
create_table_staging_events >> end_operator

start_operator >> create_table_staging_songs
create_table_staging_songs >> end_operator

start_operator >> create_table_users
create_table_users >> end_operator

start_operator >> create_table_time
create_table_time >> end_operator
