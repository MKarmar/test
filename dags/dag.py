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
    
    ''''
    Validate that the JSON schema is indeed correct before processing. This is done, to ensure that changes in source system data generation is in sync with current expected data model

    After the validation, steps can be taken to generate a new JSON schema file, in order to have a snapshot of current state of the source system expected output schema. Further
    the system admin and other stakeholders should be notified that the schema has changed. This might cause them to want to roll back a source system update or take similar actions.
    '''
    validate_input_towards_JSON = DummyOperator(task_id='validate_input_towards_JSON')
    
    if_valid_json = DummyOperator(task_id='if_valid_json')
    
    if_not_valid_json = DummyOperator(task_id='if_not_valid_json')
    
    generate_new_json_schema = DummyOperator(task_id='generate_new_json_schema')

    notify_schema_change = DummyOperator(task_id='notify_schema_change')

    generate_new_json_schema = DummyOperator(task_id='generate_new_json_schema')
    
    '''
    This set of operations is to ensure good data quality before data is releases. Data entries with questionable data quality will be stored in individual data set, but will follow
    the same data model as the validated data, in order for users to unify the data, if there is a need for doing so.
    '''
    validate_against_erp = DummyOperator(task_id='validate_against_erp') #ensure that ID's, SKU's and locations are correct in the individual records
    number_validation = DummyOperator(task_id='number_validation') #ensure that availableStockLevel values are of correct type (i.e. non negative integers)
    split_datasets = DummyOperator(task_id='split_datasets')
    write_data = DummyOperator(task_id='write_data')


    
    release_dataset = DummyOperator(task_id='release_dataset')
    
    _ = wait_for_full_data >> validate_input_towards_JSON >> if_valid_json >>validate_against_erp>>number_validation>>split_datasets>>write_data
    _ = validate_input_towards_JSON >> if_not_valid_json >> generate_new_json_schema >> notify_schema_change >> generate_new_json_schema

dag = create_dag()
