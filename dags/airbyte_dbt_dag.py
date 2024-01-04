from airflow import DAG

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

import pendulum

from cosmos import DbtTaskGroup, ProjectConfig,ExecutionConfig,RenderConfig 

from cosmos.config import ProfileConfig 

import os 

# airbyte settings
AIRBYTE_CONNECTION_ID = 'b0083742-1a65-4184-aa2c-c16c1fc0d830' # replace this with your airbyte connection id
AIRBYTE_CONNECTION_NAME = 'airflow-airbyte', # replace this with the name of your airbyte connection

# dbt settings
PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_tutorial",
)

PROFILE_CONFIG = ProfileConfig ( 
        profile_name = "dbt_clickhouse_demo", 
        target_name = "dev", 
        profiles_yml_filepath = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_tutorial/profiles.yml" 
        ) 

EXECUTION_CONFIG=ExecutionConfig( 
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
        )

# dag
with DAG(dag_id='airbyte_dbt_dag',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

   trigger_airbyte_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_sync',
       airbyte_conn_id=AIRBYTE_CONNECTION_NAME,
       connection_id=AIRBYTE_CONNECTION_ID,
       asynchronous=True
   )

   wait_for_sync_completion = AirbyteJobSensor(
       task_id='airbyte_check_sync',
       airbyte_conn_id=AIRBYTE_CONNECTION_NAME,
       airbyte_job_id=trigger_airbyte_sync.output
   )

   run_dbt_models = DbtTaskGroup(
        group_id = 'run_dbt_models',
        project_config=PROJECT_CONFIG, 
        profile_config=PROFILE_CONFIG, 
        execution_config=EXECUTION_CONFIG, 
        render_config=RenderConfig(dbt_deps = False ) 
        )


trigger_airbyte_sync >> wait_for_sync_completion >> run_dbt_models

