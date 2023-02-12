from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta
from prefect.tasks import task_input_hash
import os


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url : str) -> pd.DataFrame:
    """read taxi data from web to pandas data frame"""

    df= pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df : pd.DataFrame, color : str) -> pd.DataFrame:
    """fix dtype issues"""
    if color == "yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    if color == "green":
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    if color == "fhv":
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df : pd.DataFrame, color : str, dataset_file : str) -> Path:
    """write dataframe out locally as a parquet file"""
    wd = os.getcwd()
    path = Path(f"{wd}/data/{color}/{dataset_file}.csv.gz")
    to_path = Path(f"data/{color}/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path, to_path


@task()
def write_gcs(path : Path, to_path : Path) -> None:
    """upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=to_path, 
        timeout=120
    )
    return



@flow(log_prints=True)
def etl_web_to_gcs(color : str, year : int, month : int) -> None:
    """the main etl function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    print(f"this flow processes {len(df_clean)} rows")
    path, to_path = write_local(df_clean, color, dataset_file)
    write_gcs(path, to_path)


@flow()
def etl_parent_flow(color: str = "green", year: int = 2020, months: list[int] = [11]):
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == '__main__':
    color = "fhv"
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2019
    etl_parent_flow(color, year, months)