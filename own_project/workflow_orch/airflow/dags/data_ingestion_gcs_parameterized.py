import os
import logging
from datetime import datetime

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

### clean up into separate module later

def format_file_name(filename): 
    if filename.endswith('.csv.gz'):
        filename.replace('.csv.gz', '.parquet')
    else:
        filename.replace('.csv', '.parquet')
    return filename 

url_prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
dataset_file_yellow = "yellow_tripdata_{{ execution_date.strftime(\"%Y-%m\") }}.csv.gz"
yellow_dataset_url = f"{url_prefix}/yellow/{dataset_file_yellow}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = format_file_name(dataset_file_yellow)

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


### clean up into separate module later

def format_to_parquet(src_file):
    if not src_file.endswith(('.csv', '.csv.gz')):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    if src_file.endswith('.csv.gz'):
        pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))
    else:
        pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_historical_load",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    tags=['dtc-de'],
) as dag:
    
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {yellow_dataset_url} > {path_to_local_home}/{dataset_file_yellow}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs=dict(
            src_file=f"{path_to_local_home}/{dataset_file_yellow}",
        ),
    )


    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs=dict(
            bucket=BUCKET,
            object_name=f"raw/{parquet_file}",
            local_file=f"{path_to_local_home}/{parquet_file}",
        ),
    )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {path_to_local_home}/{dataset_file_yellow}"
    )


download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task