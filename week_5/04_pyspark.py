import pyspark
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

#df = spark.read \
    #.option("header", "true") \
    #.csv("fhvhv_tripdata_2021-01.csv.gz")

#print(df.head(5))

df_2 = pd.read_csv("fhvhv_tripdata_2021-01.csv.gz")
print(df_2.head(5))

spark.createDataFrame(df_2).show()