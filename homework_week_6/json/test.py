import pandas as pd

df = pd.read_csv('../data/green_tripdata_2019-01.csv')

df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])

print(df.dtypes)
print(df.head(5))

df.to_csv('../data/green_tripdata_2019-01.csv')
