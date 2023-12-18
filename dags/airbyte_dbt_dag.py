from airflow import DAG

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

import pendulum

AIRBYTE_CONNECTION_ID = 'b0083742-1a65-4184-aa2c-c16c1fc0d830' # replace this with your airbyte connection id
AIRBYTE_CONNECTION_NAME = 'airflow-airbyte', # replace this with the name of your airbyte connection

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

   trigger_airbyte_sync >> wait_for_sync_completion
