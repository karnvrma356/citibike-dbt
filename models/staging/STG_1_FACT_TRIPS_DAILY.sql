{{ config(materialized='view') }}

with trips as (
  select *
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
  where is_valid = true
    and is_duplicate = false
),

weather as (
  select *
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where is_valid = true
),

trip_daily as (
  select
    trip_date                                   as trip_date,
    to_number(to_char(trip_date,'YYYYMMDD'))    as date_key,

    count(*)                                    as trip_count,
    avg(tripduration_sec)                       as avg_duration_sec,
    avg(distance_km)                            as avg_distance_km,
    sum(distance_km)                            as total_distance_km
  from trips
  group by all
),

weather_daily as (
  select
    weather_date                                as trip_date,
    to_number(to_char(weather_date,'YYYYMMDD')) as date_key,

    /* NYC constant-like keys for context (still fine) */
    max(city_id)                                as city_id,
    max_by(weather_id, src_load_ts_utc)         as weather_id,

    upper(coalesce(weather_condition,'UNKNOWN')) as weather_condition,

    avg(temp_k)                                  as avg_temp_k,
    avg(humidity_pct)                            as avg_humidity_pct,
    avg(cloud_pct)                               as avg_cloud_pct,
    avg(wind_speed)                              as avg_wind_speed,

    max(src_load_ts_utc)                         as src_load_ts_utc,
    max(src_load_ts_nz)                          as src_load_ts_nz
  from weather
  group by all
),

final as (
  select
    /* business keys (TOP) */
    td.date_key,
    td.trip_date,
    wd.weather_condition,
    wd.city_id,
    wd.weather_id,

    /* measures */
    td.trip_count,
    td.avg_duration_sec,
    td.avg_distance_km,
    td.total_distance_km,

    wd.avg_temp_k,
    wd.avg_humidity_pct,
    wd.avg_cloud_pct,
    wd.avg_wind_speed,

    /* metadata (END) */
    wd.src_load_ts_utc,
    wd.src_load_ts_nz,
    current_timestamp()::timestamp_ntz     as stg_load_ts_utc,
    {{ to_nz('current_timestamp()') }}     as stg_load_ts_nz
  from trip_daily td
  left join weather_daily wd
    on td.date_key = wd.date_key
)

select * from final
