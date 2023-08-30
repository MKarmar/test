from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import PythonOperator

from data_transformer import transform_data_from_json_records
from data_quality_assess import data_quality_assess
from release_data import release_data

default_args = {
    'owner': 'data_engineering_squad',
    'retries': 0,
    'retry_delay': timedelta(minutes=20),
    'execution_timeout': timedelta(hours=4),
    'priority_weight': 1,
    'start_date': datetime(2023, 3, 1),
    'end_date': datetime(2024, 3, 3),
}

@dag(
        dag_id='sctoc_change_dag',
        default_args=default_args,
        schedule_interval='@daily',
        dagrun_timeout=timedelta(hours=2),
        tags=['data_engineering', 'deployment'],
        catchup=True,
        max_active_runs=1
)
def create_dag():
    
    wait_for_full_data = TimeDeltaSensor(task_id='wait_for_data',
                                         execution_timeout=timedelta(hours=2),
                                         delta=timedelta(hours=1),
                                         poke_interval=timedelta(minutes=10).total_seconds(),
                                         mode='reschedule')
    
    flatten_date = PythonOperator(
        task_id='flatten_data',
        python_callable=transform_data_from_json_records,
        provide_context=True
    )

    quality_asses = PythonOperator(
        task_id='quality_asses',
        python_callable=data_quality_assess,
        provide_context=True
    )

    release_dataset = PythonOperator(
        task_id='release_curret_stock_report',
        python_callable=release_data,
        provide_context=True
    ) 

    wait_for_full_data>>flatten_date>>quality_asses>>release_dataset
dag = create_dag()
