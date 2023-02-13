CREATE OR REPLACE EXTERNAL TABLE `sublime-forest-375816.trips_data_all.external_fhv_tripdata`
OPTIONS(
  format = 'CSV',
  uris = ['gs://dtc_data_lake_sublime-forest-375816/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

SELECT COUNT(int64_field_0) AS number_of_rows
FROM `sublime-forest-375816.trips_data_all.fhv_nonpartitioned_tripdata`;

CREATE OR REPLACE TABLE `sublime-forest-375816.trips_data_all.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `sublime-forest-375816.trips_data_all.external_fhv_tripdata`;

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `sublime-forest-375816.trips_data_all.fhv_nonpartitioned_tripdata`;

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `sublime-forest-375816.trips_data_all.external_fhv_tripdata`;

SELECT COUNT(int64_field_0) 
FROM `sublime-forest-375816.trips_data_all.fhv_nonpartitioned_tripdata`
WHERE PUlocationID IS NULL
AND DOlocationID IS NULL;

CREATE OR REPLACE TABLE `sublime-forest-375816.trips_data_all.fhv_partitioned_tripdata`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `sublime-forest-375816.trips_data_all.external_fhv_tripdata`;

SELECT COUNT (DISTINCT Affiliated_base_number)
FROM `sublime-forest-375816.trips_data_all.fhv_partitioned_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT COUNT (DISTINCT Affiliated_base_number)
FROM `sublime-forest-375816.trips_data_all.fhv_nonpartitioned_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';