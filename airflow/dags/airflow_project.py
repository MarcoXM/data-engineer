#### setting start time and record history
from datetime import datetime, timedelta 
import os

#### Airflow 
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)

#### Sql query
from helpers import SqlQueries as sq


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 1),
    'email': ['xwang423@fordham.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('DataEngineering_airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *' # every xx:15 
        )
start_date = datetime.utcnow()
start_operator = DummyOperator( # It is like a linked list, the first Node is a Dummy one
    task_id='Begin_execution',
    dag=dag) 

create_redshift_tables = CreateTableOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift",
    sqls = '../create_tables.sql'
)


## loading event data to redshift datawarehouse
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON",
    execution_date=start_date
)

## do something within the REDSHIFT Cluster
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=sq.songplay_table_insert
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context = True,
    aws_credentials_id = 'aws_credentials_id', # AWS admin role
    
    redshift_conn_id = 'redshift',
    sql_query = sq.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context = True,
    aws_credentials_id = 'aws_credentials_id', # AWS admin role
    
    redshift_conn_id = 'redshift',
    sql_query = sq.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    
    provide_context = True,
    aws_credentials_id = 'aws_credentials_id', # AWS admin role
    
    redshift_conn_id = 'redshift',
    sql_query = sq.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=sq.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["songplays", "users", "s", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator


