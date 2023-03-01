import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
import pandas as pd


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True),
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('data/raw/fhvhv/2021/06') \

df.show()
df.printSchema()

df \
    .repartition(12) \
    .write.parquet('data/pq/fhvhv/2021/06/')