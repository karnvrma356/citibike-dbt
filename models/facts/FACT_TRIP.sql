{{ config(materialized='table') }}

with t as (
  select *
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
),

dim_station as (
  select station_id, station_sk
  from {{ ref('DIM_STATION') }}
),

dim_user as (
  select user_type_raw, user_type_sk
  from {{ ref('DIM_USER_TYPE') }}
),

bridge as (
  select trip_sk, dim_weather_sk
  from {{ ref('BRG_TRIP_WEATHER_AT_START') }}
),

dim_weather as (
  select weather_sk, weather_id, weather_main, weather_desc
  from {{ ref('DIM_WEATHER') }}
),

/* map dim_weather_sk -> weather_sk via weather_id/main/desc from STG weather table */
weather_lookup as (
  select
    w.dim_weather_sk,
    d.weather_sk
  from {{ ref('STG_1_DIM_WEATHER_NYC') }} w
  join dim_weather d
    on d.weather_id = w.weather_id
   and coalesce(d.weather_main,'') = coalesce(w.weather_main,'')
   and coalesce(d.weather_desc,'') = coalesce(w.weather_desc,'')
  qualify row_number() over (partition by w.dim_weather_sk order by w.src_load_ts_utc desc) = 1
)

select
  /* keys */
  t.trip_sk,
  to_number(to_char(t.trip_date,'YYYYMMDD')) as date_key,

  coalesce(ds1.station_sk, sha2('UNKNOWN',256)) as start_station_sk,
  coalesce(ds2.station_sk, sha2('UNKNOWN',256)) as end_station_sk,
  coalesce(du.user_type_sk, sha2('UNKNOWN',256)) as user_type_sk,

  /* weather FK */
  coalesce(wl.weather_sk, sha2('-1',256)) as weather_sk,

  /* dates */
  t.trip_date,
  t.start_time,
  t.stop_time,

  /* degenerate */
  t.bike_id,
  t.birth_year,
  t.gender,

  /* measures */
  t.tripduration_sec,
  t.duration_seconds_calc,
  t.distance_km,

  /* metadata */
  t.file_name,
  t.row_num,
  t.src_load_ts_utc,
  t.src_load_ts_nz,
  current_timestamp()::timestamp_ntz as fact_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as fact_load_ts_nz

from t
left join dim_station ds1 on t.start_station_id = ds1.station_id
left join dim_station ds2 on t.end_station_id   = ds2.station_id
left join dim_user du     on t.user_type = du.user_type_raw
left join bridge b        on t.trip_sk = b.trip_sk
left join weather_lookup wl on b.dim_weather_sk = wl.dim_weather_sk