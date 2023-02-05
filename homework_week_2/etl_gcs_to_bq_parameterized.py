from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.cloud_storage import GcpCredentials


@task(retries=3)
def extract_from_gcs(color : str, year : int, month : int) -> Path:
    """download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=gcs_path
        )
    return Path(f"{gcs_path}")


@task()
def read_local_write_bq(path : Path) -> None:
    """Write dataframe to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")
    
    df = pd.read_parquet(path)
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="sublime-forest-375816",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )



@flow(log_prints=True)
def etl_gcs_to_bq() -> None:
    """the main etl flow to load data to big query"""
    color="yellow"
    year=2021
    month=1

    path = extract_from_gcs(color, year, month)
    read_local_write_bq(path)


if __name__ == '__main__':
    etl_gcs_to_bq()