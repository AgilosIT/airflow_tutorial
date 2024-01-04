from pendulum import datetime 
from airflow import DAG 
from airflow.operators.empty import EmptyOperator 
from cosmos import DbtTaskGroup, ProjectConfig,ExecutionConfig,RenderConfig 
from cosmos.config import ProfileConfig 
import os 

from cosmos import DbtDag

import pendulum

project_config = ProjectConfig(
    dbt_project_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_tutorial",
)

profile_config = ProfileConfig ( 
        profile_name = "dbt_clickhouse_demo", 
        target_name = "dev", 
        profiles_yml_filepath = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_tutorial/profiles.yml" 
        ) 

with DAG(dag_id='dbt_dag',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

    e1 = EmptyOperator(task_id="pre_dbt") 

    dbt_tg = DbtTaskGroup( 
                project_config=ProjectConfig(dbt_project_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/dbt_tutorial"), 
                profile_config=profile_config, 
                execution_config=ExecutionConfig( 
                    dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
                ), 
                render_config=RenderConfig(dbt_deps = False ) 
                ) 
 
    e2 = EmptyOperator(task_id="post_dbt") 

e1 >> dbt_tg >> e2