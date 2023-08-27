from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor

default_args = {
    'owner': 'data_engineering_squad',
    'retries': 3,
    'retry_delay': timedelta(minutes=20),
    'execution_timeout': timedelta(hours=4),
    'priority_weight': 1,
    'start_date': datetime(2023, 3, 1),
    'end_date': datetime(2023, 3, 3),
}

@dag(
        dag_id='deployment_dag',
        default_args=default_args,
        schedule_interval='@daily',
        dagrun_timeout=timedelta(hours=2),
        tags=['data_engineering', 'deployment'],
        catchup=True,
        max_active_runs=1,
)
def create_dag():
    
    wait_for_full_data = TimeDeltaSensor(task_id='wait_for_data',
                                         execution_timeout=timedelta(hours=2),
                                         delta=timedelta(hours=1),
                                         poke_interval=timedelta(minutes=10).total_seconds(),
                                         mode='reschedule')
    
    release_dataset = DummyOperator(task_id='release_dataset')
    
    _ = wait_for_full_data >> release_dataset

dag = create_dag()
