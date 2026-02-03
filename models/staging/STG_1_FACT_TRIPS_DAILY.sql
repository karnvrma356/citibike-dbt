{{ config(materialized='view') }}

with trips as (
  select
    trip_sk,
    trip_date,
    to_number(to_char(trip_date,'YYYYMMDD')) as date_key,
    duration_seconds_calc,
    distance_km
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
),

bridge as (
  select
    trip_sk,
    dim_weather_sk
  from {{ ref('BRG_TRIP_WEATHER_AT_START') }}
),

weather_obs as (
  select
    dim_weather_sk,
    weather_id,
    weather_main,
    weather_desc,
    upper(coalesce(weather_condition,'UNKNOWN')) as weather_condition,
    temp_k,
    humidity_pct,
    cloud_pct,
    wind_speed,
    src_load_ts_utc,
    src_load_ts_nz
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
),

/* Join trips -> bridge -> weather observation */
trip_weather as (
  select
    t.trip_date,
    t.date_key,

    t.duration_seconds_calc,
    t.distance_km,

    w.weather_id,
    w.weather_main,
    w.weather_desc,
    coalesce(w.weather_condition, 'UNKNOWN') as weather_condition,

    w.temp_k,
    w.humidity_pct,
    w.cloud_pct,
    w.wind_speed,

    w.src_load_ts_utc,
    w.src_load_ts_nz
  from trips t
  left join bridge b
    on t.trip_sk = b.trip_sk
  left join weather_obs w
    on b.dim_weather_sk = w.dim_weather_sk
),

/* Map to DIM_WEATHER so FACT has a proper key */
dim_weather as (
  select
    weather_sk,
    weather_id,
    weather_main,
    weather_desc
  from {{ ref('DIM_WEATHER') }}
),

daily as (
  select
    tw.trip_date,
    tw.date_key,

    coalesce(dw.weather_sk, sha2('-1',256)) as weather_sk,
    tw.weather_condition,

    count(*) as trip_count,
    avg(tw.duration_seconds_calc) as avg_duration_sec,
    avg(tw.distance_km) as avg_distance_km,
    sum(tw.distance_km) as total_distance_km,

    avg(tw.temp_k) as avg_temp_k,
    avg(tw.humidity_pct) as avg_humidity_pct,
    avg(tw.cloud_pct) as avg_cloud_pct,
    avg(tw.wind_speed) as avg_wind_speed,

    max(tw.src_load_ts_utc) as src_load_ts_utc,
    max(tw.src_load_ts_nz)  as src_load_ts_nz,

    current_timestamp()::timestamp_ntz as stg_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as stg_load_ts_nz
  from trip_weather tw
  left join dim_weather dw
    on dw.weather_id = tw.weather_id
   and coalesce(dw.weather_main,'') = coalesce(tw.weather_main,'')
   and coalesce(dw.weather_desc,'') = coalesce(tw.weather_desc,'')
  group by
    tw.trip_date,
    tw.date_key,
    coalesce(dw.weather_sk, sha2('-1',256)),
    tw.weather_condition
)

select * from daily
