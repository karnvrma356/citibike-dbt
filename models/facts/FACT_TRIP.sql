{{ config(materialized='table') }}

with t as (
  select *
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
  where is_valid = true
    and is_duplicate = false
),

dim_station as (
  select station_id, station_sk
  from {{ ref('DIM_STATION') }}
),

weather as (
  select *
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where is_valid = true
),

dim_user as (
  select user_type, user_type_sk
  from {{ ref('DIM_USER_TYPE') }}
),

final as (
  select
    /* keys (TOP) */
    to_number(to_char(t.trip_date,'YYYYMMDD')) as date_key,
    coalesce(ds1.station_sk, sha2('-1',256))   as start_station_sk,
    coalesce(ds2.station_sk, sha2('-1',256))   as end_station_sk,
    coalesce(du.user_type_sk, sha2('UNKNOWN',256)) as user_type_sk,
    sha2(
      to_varchar(weather_id) || '|' ||
      coalesce(weather_main,'') || '|' ||
      coalesce(weather_desc,''),
      256
    ) as weather_sk,

    /* dates */
    t.trip_date,
    t.start_time,
    t.stop_time,

    /* measures */
    t.tripduration_sec,
    t.distance_km,

    /* metadata (END) */
    t.src_load_ts_utc,
    t.src_load_ts_nz,
    current_timestamp()::timestamp_ntz as fact_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as fact_load_ts_nz
  from t
  left join dim_station ds1 on t.start_station_id = ds1.station_id
  left join dim_station ds2 on t.end_station_id   = ds2.station_id
  left join dim_user du     on upper(coalesce(t.user_type,'UNKNOWN')) = du.user_type
  left join weather w on t.trip_date = w.weather_date
  )

select * from final
