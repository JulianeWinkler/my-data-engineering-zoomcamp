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

### for green taxi data

green_schema = types.StructType([
    types.StructField("VendorID", types.IntegerType(), True),
    types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("ehail_fee", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("payment_type", types.IntegerType(), True),
    types.StructField("trip_type", types.IntegerType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True)
])

year = 2020

for month in range (1,13):
    print(f'processing data for {year}/{month}')

    input_path = f'data/raw/green/{year}/{month:02d}/'
    output_path = f'data/pq/green/{year}/{month:02d}/'
    df_green = spark.read \
        .option("header", "true") \
        .schema(green_schema) \
        .csv(input_path) \

    df_green.printSchema()
    
    df_green \
        .repartition(4) \
        .write.parquet(output_path)
