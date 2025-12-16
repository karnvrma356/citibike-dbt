{{ config(materialized='view') }}

with weather_daily as (
  select
    weather_date as trip_date,
    weather_condition,

    /* context numeric columns */
    avg(temp_k) as avg_temp_k,
    avg(humidity_pct) as avg_humidity_pct,
    avg(cloud_pct) as avg_cloud_pct,
    avg(wind_speed) as avg_wind_speed
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  group by weather_date, weather_condition
)

select
  /* keys */
  sha2(concat_ws('|', t.trip_date::string, w.weather_condition), 256) as date_weather_sk,
  d.date_key,

  /* dates */
  t.trip_date,

  /* descriptions */
  w.weather_condition,

  /* measures */
  t.trip_count,
  t.avg_duration_sec,
  t.total_distance_km,
  t.avg_distance_km,

  /* context numeric columns (still in fact) */
  w.avg_temp_k,
  w.avg_humidity_pct,
  w.avg_cloud_pct,
  w.avg_wind_speed,

  /* metadata (END) */
  current_timestamp()                    as stg_load_ts_utc,
  {{ to_nz('current_timestamp()') }}     as stg_load_ts_nz

from {{ ref('STG_1_FACT_TRIPS_DAILY') }} t
join weather_daily w
  on w.trip_date = t.trip_date
join {{ ref('DIM_DATE') }} d
  on d.date_day = t.trip_date
