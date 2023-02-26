import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(spark.version)
print(pyspark.__file__)
print(spark.sparkContext.uiWebUrl)

df = spark.read \
    .option("header", "true") \
    .csv('/home/juliane/my-data-engineering-zoomcamp/week_5/taxi+_zone_lookup.csv')

df.show()

#df.write.parquet('zones')
