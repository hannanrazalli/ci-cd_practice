import os
from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "transactions-practice")
GCP_DATASET = os.getenv("GCP_DATASET_BRONZE", "bronze")
GCP_LOCATION = os.getenv("GCP_LOCATION", "asia-southeast1")
IS_PROD = os.getenv("IS_PRODUCTION", "False").lower() == "true"
DBT_PROJECT_PATH = Path(os.getenv("AIRFLOW_HOME", "/usr/local/airflow")) /"include/dbt/oms_dbt_proj"

profile_config = ProfileConfig(
    profile_name = "oms_dbt_proj",
    target_name = "dev",
    profile_mapping = GoogleCloudServiceAccountDictProfileMapping(
        conn_id = "google_cloud_default",
        profile_args = {
            "project" : GCP_PROJECT,
            "dataset" : GCP_DATASET,
            "location" : GCP_LOCATION,
        },
    ),
)

dbt_dag = DbtDag(
    project_config = ProjectConfig(DBT_PROJECT_PATH),
    operator_args = {
        "install_deps" : True,
        "full_refresh" : not IS_PROD
    },
    profile_config = profile_config,
    render_config = RenderConfig(
        load_method = LoadMode.DBT_LS,
        dbt_deps = True
    ),
    schedule = "0 16 * * *",
    start_date = datetime(2024, 1, 1),
    catchup = False,
    dag_id = "medallion_practice",
    default_args = {
        "owner": "hannan_razalli",
        "retries": 2
        }
)