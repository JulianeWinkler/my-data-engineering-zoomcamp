import os
from time import time
import pandas as pd
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
   print(table_name, csv_file, execution_date, user, password)
   

   engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
   engine.connect()

   print('connection established successfully, inserting data...')
   
   t_start = time()
   df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
   df = next(df_iter)
   if 'yellow' in csv_file:
      df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
      df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
   else:
      print('no transformations necessary')

   df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
   df.to_sql(name=table_name, con=engine, if_exists='append')

   t_end = time()
   print('inserted the first chunk, took %.3f second' % (t_end - t_start))

   while True:
      try:
         t_start = time()

         df = next(df_iter)
         if 'tripdata' in url:
            df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
            df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
         else:
            print('no transformations necessary')
              
         df.to_sql(name=table_name, con=engine, if_exists='append')

         t_end = time()
         
         print('uploaded another chunk, took %.3f seconds' %(t_end - t_start))
      except StopIteration:
         print('Finished ingesting data into the postgres database')
         break

