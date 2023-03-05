import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

schema = types.StructType([
    types.StructField('LocationID', types.IntegerType(), False),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True),
])

df_zones = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('data/taxi+_zone_lookup.csv')

df_zones.show()
df_zones.printSchema()

df_fhv = spark.read.parquet('data/pq/fhvhv/2021/06/')

df_fhv.show()
df_fhv.printSchema()

df_fhv.createOrReplaceTempView('fhv_trips_data')
df_zones.createOrReplaceTempView('zones_data')

spark.sql("""
SELECT
    COUNT(1) AS number_trips
    ,pz.Zone
    FROM fhv_trips_data fh
    INNER JOIN zones_data pz ON pz.LocationID = fh.PULocationID
    INNER JOIN zones_data dz ON dz.LocationID = fh.DOLocationID
    GROUP BY pz.Zone
    ORDER BY number_trips DESC
""").show()