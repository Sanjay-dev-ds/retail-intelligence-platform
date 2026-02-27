import os
from datetime import datetime
from pathlib import Path

from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig , DbtDag
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

# DBT project configuration
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_retail"
execution_config = ExecutionConfig(
    dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
)

profile_config = ProfileConfig(
    profile_name="retail_bigquery",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": "project-4d474cb9-a1a5-457f-80b",
            "dataset": "retail_bronze"        },
    ),
)


retail_dbt_medallion = DbtDag(
    dag_id="retail_dbt_medallion",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 3 * * *",  # run after bronze load (2 AM) completes
    catchup=False,
    tags=["retail", "dbt", "cosmos", "medallion"],
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
)


dag  = retail_dbt_medallion