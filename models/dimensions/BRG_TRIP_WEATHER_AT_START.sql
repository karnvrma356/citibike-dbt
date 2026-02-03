{{ config(materialized='table') }}

with weather_points as (
  select
    dim_weather_sk,
    weather_datetime,
    lead(weather_datetime) over (order by weather_datetime) as next_weather_datetime
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where weather_datetime is not null
    and city_id = 5128638
),

weather_effective as (
  select
    dim_weather_sk,
    weather_datetime,
    coalesce(next_weather_datetime, dateadd('hour', 6, weather_datetime)) as next_weather_datetime
  from weather_points
),

weather_window as (
  select
    min(weather_datetime) as min_weather_dt,
    max(weather_datetime) as max_weather_dt
  from weather_effective
),

trips as (
  select
    t.trip_sk,
    t.start_time
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }} t
  join weather_window w on 1=1
  where t.start_time is not null
    and t.start_time >= w.min_weather_dt
    and t.start_time <= w.max_weather_dt
),

matched as (
  select
    t.trip_sk,
    w.dim_weather_sk,
    w.weather_datetime as matched_weather_datetime,
    current_timestamp()::timestamp_ntz as map_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as map_load_ts_nz,

    row_number() over (
      partition by t.trip_sk
      order by w.weather_datetime desc
    ) as rn
  from trips t
  join weather_effective w
    on t.start_time >= w.weather_datetime
   and t.start_time <  w.next_weather_datetime
)

select
  trip_sk,
  dim_weather_sk,
  matched_weather_datetime,
  map_load_ts_utc,
  map_load_ts_nz
from matched
qualify rn = 1