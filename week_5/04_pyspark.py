import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
"""
#df = spark.read \
    .option("header", "true") \
    .csv("fhvhv_tripdata_2021-01.csv.gz")

#print(df.head(5))

schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True)
])


#df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("fhvhv_tripdata_2021-01.csv.gz")

"""

#print(df.head(5))
#df = df.repartition(24)
#df.write.parquet('fhvhv/2021/01/')

df = spark.read.parquet('fhvhv/2021/01/')
print(df)
df.printSchema()
df \
    .select('hvfhs_license_num','pickup_datetime','dropoff_datetime','PULocationID','DOLocationID') \
    .filter(df.hvfhs_license_num == 'HV0003') \
    .show()

#just like in python you can define your own functions

def crazy_stuff()

#pyspark integrated sql functions

df \
    .withColumn('pickup_date',F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date',F.to_date(df.dropoff_datetime)) \
    .show()

