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
    # Defaults inherited by tasks in this DAG unless a task overrides them.
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
}

# These lists are used with dynamic task mapping. Airflow creates one mapped
# task instance for each endpoint/table value instead of making us copy code.
API_ENDPOINTS = ["/api"] #, "/data2", "/data3"]
BQ_TABLES = ["elon"] #, "table2", "table3"]

with DAG(
    "complex_elt_pipeline_api_to_bq_dbt_cosmos",
    default_args=default_args,
    description="Complex ELT pipeline using Astronomer Cosmos with DBTTaskGroup",
    schedule=None,  # Manual trigger only; use "@daily" for a daily schedule.
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["bigquery", "api", "dbt", "cosmos"]
) as dag:

    @task()
    def extract_data_from_api(endpoint: str):
        """
        Extract data from an API endpoint and save to a local JSON file.
        """
        # The connection stores the API base URL and any authentication details.
        http_hook = HttpHook(method="GET", http_conn_id="elon_api")
        response = http_hook.run(endpoint=endpoint)
        response_data = response.json()

        # Each mapped task writes its own file, for example /tmp/api_data_data1.json.
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
        # The API returns one JSON object with a required `source` field.
        if not data or "source" not in data:
            raise ValueError(f"Data validation failed for file: {file_path}")
        return file_path

    extracted_files = extract_data_from_api.expand(endpoint=API_ENDPOINTS)
    validated_files = validate_data.expand(file_path=extracted_files)

    # Upload each validated local JSON file to Google Cloud Storage (GCS).
    # `.partial()` sets arguments shared by every mapped upload task.
    upload_to_gcs = LocalFilesystemToGCSOperator.partial(
        # The base task id; Airflow adds a map index to each generated instance.
        task_id="upload_to_gcs",
        # Airflow connection containing Google Cloud credentials.
        gcp_conn_id="gcp",
        # The destination GCS bucket. This bucket must already exist and be
        # accessible by the Google connection used by Airflow.
        bucket="bucket-elon",
        # Use the map index to give each upload a stable, unique GCS object name.
        dst="api_data_{{ ti.map_index }}.json",
    ).expand(
        # `.expand()` creates one upload for each value produced by
        # validate_data. `src` is the local path to upload.
        src=validated_files
    )

    # Load the uploaded GCS objects into BigQuery.
    # As above, `.partial()` contains settings shared by every mapped load.
    gcs_to_bigquery = GCSToBigQueryOperator.partial(
        task_id="gcs_to_bigquery",
        # Use the same Google Cloud connection as the GCS upload.
        gcp_conn_id="gcp",
        # The same GCS bucket used by the upload step.
        bucket="bucket-elon",
        source_format="NEWLINE_DELIMITED_JSON",
        # BigQuery uses this schema when creating or loading the destination
        # table. REQUIRED means every row must have a source; NULLABLE allows
        # the other columns to be absent.
        schema_fields=[
            {"name": "source", "type": "STRING", "mode": "REQUIRED"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "urlImage", "type": "STRING", "mode": "NULLABLE"},
            {"name": "publishDate", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
        # Replace all existing rows in each destination table on every run.
        # Use WRITE_APPEND instead if new API records should be added instead.
        write_disposition="WRITE_APPEND",
        # Add BigQuery's ingestion-time daily partitioning to each table.
        # time_partitioning={"type": "DAY"},
    ).expand(
        # Read the object created by the corresponding mapped upload task.
        source_objects=["api_data_{{ ti.map_index }}.json"],
        # Fully qualified BigQuery table names use project.dataset.table.
        # Each entry here is paired with a mapped source object by Airflow.
        destination_project_dataset_table=[
            f"retail-dbt-airflow.elon.{table}" for table in BQ_TABLES
        ],
        )

    # project_config = ProjectConfig(
    #     dbt_project_path=(DBT_ROOT_PATH / "jaffle_shop").as_posix(),
    # )

    # profile_config = ProfileConfig(
    #     profile_name="default",
    #     target_name="dev",
    #     profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
    #         conn_id="gcp_default",
    #         profile_args={"schema": "public"},
    #     )
    # )

    # jaffle_shop = DbtTaskGroup(
    #     group_id="jaffle_shop",
    #     project_config=project_config,
    #     execution_config=ExecutionConfig(dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"),
    #     ProfileConfig = 
    #     default_args = {"retries": 2},
    #     dag=dag,
    # )

    # notify_slack = SlackWebhookOperator(
    #     task_id="notify_slack_success",
    #     slack_webhook_conn_id="slack"
    #     http_conn_id="slack_webhook",
    #     message="The ELT pipeline successfully completed",
    # )

    upload_to_gcs >> gcs_to_bigquery