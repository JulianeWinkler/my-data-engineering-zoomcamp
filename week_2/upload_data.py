import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
from prefect import flow, task

@task(log_prints=True, retries=2)
def ingest_data(user, password, host, port, db, table_name, url):
   
   if url.endswith('.csv.gz'):
      csv_name = 'output.csv.gz'
   else:
      csv_name = 'output.csv'
   
   os.system(f"wget {url} -O {csv_name}")

   engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

   
   df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
   df = next(df_iter)
   if 'tripdata' in url:
      df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
      df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
   else:
      print('no transformations necessary')

   df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
   df.to_sql(name=table_name, con=engine, if_exists='append')

   while True:
      try:
         t_start = time()

         df = next(df_iter)
         if 'tripdata' in url:
            df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
            df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
         else:
            print('no transformations necessary')
              
         df.to_sql(name=table_name, con=engine, if_exists='append')

         t_end = time()
         
         print('uploaded another chunk, took %.3f seconds' %(t_end - t_start))
      except StopIteration:
         print('Finished ingesting data into the postgres database')
         break

@flow(name="ingest_flow")
def main_flow():
   user = "root"
   password = "root"
   host = "localhost"
   port = "5432"
   db = "ny_taxi"
   table_name = "yellow_taxi_trips"
   csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

   ingest_data(user, password, host, port, db, table_name, csv_url)


if __name__ == '__main__':
   main_flow()