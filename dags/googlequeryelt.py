from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.http.hooks import HttpHook
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
    tags=["bigquery",, "dbt", "cosmos"]
)