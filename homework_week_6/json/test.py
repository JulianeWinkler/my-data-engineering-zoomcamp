import pandas as pd

df = pd.read_csv('../data/fhv_tripdata_2019-01.csv.gz')

#df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
#df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

print(f"pre: na in rows: {df['PUlocationID'].isna().sum()}")
print(f"pre: na in rows: {df['DOlocationID'].isna().sum()}")

df.dropna(subset=['PUlocationID'], inplace=True)
df.dropna(subset=['DOlocationID'], inplace=True)

print(f"post: na in rows: {df['PUlocationID'].isna().sum()}")
print(f"post: na in rows: {df['DOlocationID'].isna().sum()}")

df['PUlocationID'] = df['PUlocationID'].astype(int)
df['DOlocationID'] = df['DOlocationID'].astype(int)

#print(f"pre: '' in rows: {df['PUlocationID'] == ''.sum()}")
#print(f"pre: '' in rows: {df['DOlocationID'] == ''.sum()}")

print(df.dtypes)
print(df.head(5))

df_new = df.head(2000)

df_new.to_csv('../data/fhv_tripdata_2019-01.csv', index =False)
