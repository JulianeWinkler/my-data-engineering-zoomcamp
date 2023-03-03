import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df_fhv = spark.read.parquet('data/pq/fhvhv/2021/06/')

df_fhv.show()
df_fhv.printSchema()

df_fhv = df_fhv \
    .withColumn('dropoff_date', F.to_date(df_fhv.dropoff_datetime)) \
    .withColumn('pickup_date', F.to_date(df_fhv.pickup_datetime)) \
    .withColumn('DiffInSeconds',F.col('dropoff_datetime').cast("long") - F.col('pickup_datetime').cast("long")) \
    .withColumn("DiffInHours", F.col("DiffInSeconds").cast("bigint")/3600) \


df_fhv \
    .groupBy("pickup_date") \
    .count() \
    .orderBy("pickup_date") \
    .show()

df_fhv.registerTempTable('fhv_trips_data')

spark.sql("""
SELECT
    COUNT(1) AS number_of_trips
FROM fhv_trips_data
WHERE pickup_date = '2021-06-15'
""").show()

spark.sql("""
SELECT
    MAX(DiffInHours) AS longest_trip
FROM fhv_trips_data
""").show()