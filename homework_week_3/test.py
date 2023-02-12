import os
import pandas as pd
from time import time

url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz" 
df = pd.read_csv(url)
print('read url')
print(df)
