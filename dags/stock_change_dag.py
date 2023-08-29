from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta, datetime

default_args = {
    'owner': 'data_engineering_squad',
    'retries': 3,
    'retry_delay': timedelta(minutes=20),
    'execution_timeout': timedelta(hours=4),
    'priority_weight': 1,
    'start_date': datetime(2023, 3, 1),
    'end_date': datetime(2023, 3, 3),
}
with DAG(
    dag_id="stock_change_dag",
    default_args=default_args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(hours=2),
    tags=['data_engineering', 'deployment'],
    catchup=True,
    max_active_runs=1,
) as dag:
    dummy_task_1 = DummyOperator(task_id='branch_true', dag=dag)
    dummy_task_2 = DummyOperator(task_id='branch_false', dag=dag)
    wait_for_full_data = TimeDeltaSensor(task_id='wait_for_data',
                                         execution_timeout=timedelta(hours=2),
                                         delta=timedelta(hours=1),
                                         poke_interval=timedelta(minutes=10).total_seconds(),
                                         mode='reschedule')
    
    wait_for_full_data >>dummy_task_1>>dummy_task_2