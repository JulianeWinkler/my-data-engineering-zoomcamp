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
def read_file(path : Path) -> pd.DataFrame:
    """Reading data to df"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df : pd.DataFrame) -> None:
    """Write dataframe to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")
    
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="sublime-forest-375816",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )



@flow(log_prints=True)
def etl_gcs_to_bq(color, year, month) -> None:
    """the main etl flow to load data to big query"""
    path = extract_from_gcs(color, year, month)
    df = read_file(path)
    print(f"this flow processed {len(df)} rows")
    write_bq(df)



@flow()
def etl_parent_flow_bq(color : str = "yellow", year : int = 2019, months: list[int] = [2, 3]) -> None:
    for month in months:
        etl_gcs_to_bq(color, year, month)


if __name__ == '__main__':
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow_bq(color, year, months)