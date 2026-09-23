from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
import json

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retrie": 2,
}

API_ENDPOINTS = ["/data1", "/data2", "/data3"]
BQ_TABLES = ["table1", "table2", "table3"]

with DAG(
    "complex_elt_pipeline_api_to_bq_dbt_cosmos",
    default_args=default_args,
    description="Complex ELT pipeline using Astronomer Cosmos with DBTTaskGroup",
    schedule="@daily",
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["bigquery", "api", "dbt", "cosmos"]
) as dag:

    @task()
    def extract_data_from_api(endpoint: str):
        """
        Extract data from an API endpoint and save to a local JSON file.
        """
        http_hook = HttpHook(method="GET", http_conn_id="my_api_connection")
        response = http_hook.run(endpoint=endpoint)
        response_data = response.json()

        # Save data to a local file
        local_file_path = f"/tmp/api_data_{endpoint.strip('/')}.json"
        with open(local_file_path, "w") as file:
            json.dump(response_data, file)

        return local_file_path

    @task()
    def validate_data(file_path:str):
        """
        Validate the extracted data for completene and schema conformity.
        """
        with open(file_path, "r") as file:
            data = json.load(file)
        if not data or "id" not in data[0]:
            raise ValueError(f"Data validation failed for file: {file_path}")
        return file_path

    extracted_files = extract_data_from_api.expand(endpoint=API_ENDPOINTS)
    validated_files = validate_data.expand(file_path=extracted_files)

    upload_to_gcs = LocalFilesystemToGCSOperator.partial(
        task_id=f"upload_to_gcs",
        bucket="my_gcs_bucket",
        dst="{{ ti.xcom_pull(task_ids='validate_data') | basename}}",
    ).expand(
        src=validated_files
    )

    # from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    gcs_to_bigquery = GCSToBigQueryOperator.partial(
        task_id='gcs_to_bigquery',
        bucket="my_gcs_bucket",
        
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY"},
        ).expand(
            source_objects=[
                "{{ task_instance.xcom_pull(task_ids='upload_to_gcs)}}"
            ],
            destination_project_dataset_table =[
                f"my_project.my_dataset.{table}"for table in BQ_TABLES
            ],
        )

    project_config = ProjectConfig(
        dbt_project_path=(DBT_ROOT_PATH / "jaffle_shop").as_posix(),
    )

    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
            conn_id="gcp_default",
            profile_args={"schema": "public"},
        )
    )

    jaffle_shop = DbtTaskGroup(
        group_id="jaffle_shop",
        project_config=project_config,
        execution_config=ExecutionConfig(dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"),
        ProfileConfig = 
        default_args = {"retries": 2},
        dag=dag,
    )

    notify_slack = SlackWebhookOperator(
        task_id="notify_slack_success",
        slack_webhook_conn_id="slack"
        http_conn_id="slack_webhook",
        message="The ELT pipeline successfully completed",
    )

    upload_to_gcs >> load_to_bq >> jaffle_shop >> notify_slack