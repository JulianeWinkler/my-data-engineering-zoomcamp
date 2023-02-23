{{ config(materialized='view') }}

WITH tripdata AS 
(
  SELECT 
  *
  FROM{{ source('staging','fhv_nonpartitioned_tripdata') }}
)

SELECT
    -- identifiers 
    {{dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime'])}} AS tripid,
    CAST(dispatching_base_num AS string) AS dispatching_base_num,
    CAST(Affiliated_base_number AS string) AS Affiliated_base_number,
    {{ dbt_utils.safe_cast('PUlocationid',  api.Column.translate_type("integer"))}} AS  pickup_locationid,
    {{ dbt_utils.safe_cast('DOlocationid',  api.Column.translate_type("integer"))}} AS dropoff_locationid,
    
    -- timestamps
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropOff_datetime AS timestamp) AS dropoff_datetime,
    {{ dbt_utils.safe_cast('SR_Flag',  api.Column.translate_type("integer"))}} AS SR_Flag

FROM tripdata

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}