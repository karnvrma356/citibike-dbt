{{ config(materialized='table') }}

select
  date_key,
  trip_date,
  weather_sk,
  weather_condition,

  trip_count,
  avg_duration_sec,
  avg_distance_km,
  total_distance_km,

  avg_temp_k,
  avg_humidity_pct,
  avg_cloud_pct,
  avg_wind_speed,

  src_load_ts_utc,
  src_load_ts_nz,

  current_timestamp()::timestamp_ntz as fact_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as fact_load_ts_nz
from {{ ref('STG_1_FACT_TRIPS_DAILY') }}