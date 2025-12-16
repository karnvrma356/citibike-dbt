{{ config(materialized='view', schema='STAGE') }}

with trips as (
  select *
  from CLEANSED_CITIBIKE.CORE.CITIBIKE_TRIPS_CLEAN
  where dup_rank = 1
),

weather as (
  select *
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where is_valid = true
),

/* daily trip metrics */
trip_daily as (
  select
    trip_date                                   as trip_date,
    to_number(to_char(trip_date,'YYYYMMDD'))    as date_key,

    count(*)                                    as trip_count,
    avg(tripduration_seconds)                   as avg_duration_sec,
    avg(distance_km)                            as avg_distance_km,
    sum(distance_km)                            as total_distance_km
  from (
    select
      trip_date,
      tripduration_seconds,
      /* distance already calculated in your stage/fact earlier; if not, keep column name you used */
      distance_km
    from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
    where is_valid = true
      and is_duplicate = false
  )
  group by 1,2
),

/* daily weather metrics */
weather_daily as (
  select
    weather_date                                as trip_date,
    to_number(to_char(weather_date,'YYYYMMDD')) as date_key,

    /* keep keys to build dims */
    max(city_id)                                as city_id,
    /* weather_id can vary within a day, we keep the dominant one for drill-down */
    max_by(weather_id, src_load_ts_utc)         as weather_id,

    /* map into 4-bucket condition */
    case
      when lower(weather_main) like '%rain%' or lower(weather_desc) like '%rain%' or lower(weather_desc) like '%drizzle%' then 'rain'
      when lower(weather_main) like '%snow%' or lower(weather_desc) like '%snow%' then 'snow'
      when lower(weather_main) like '%cloud%' or lower(weather_desc) like '%cloud%' then 'clouds'
      else 'clear'
    end                                          as weather_condition

    ,
    avg(temp_k)                                  as avg_temp_k,
    avg(humidity_pct)                            as avg_humidity_pct,
    avg(cloud_pct)                               as avg_cloud_pct,
    avg(wind_speed)                              as avg_wind_speed,

    max(src_load_ts_utc)                         as src_load_ts_utc,
    max(src_load_ts_nz)                          as src_load_ts_nz
  from weather
  group by 1,2
),

condition_dim as (
  select
    weather_condition_sk,
    lower(weather_condition) as weather_condition
  from MART.DIM_WEATHER_CONDITION
),

final as (
  select
    /* keys (TOP) */
    td.date_key,
    td.trip_date,
    cd.weather_condition_sk,
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
    current_timestamp()::timestamp_ntz     as fact_load_ts_utc,
    {{ to_nz('current_timestamp()') }}     as fact_load_ts_nz,
    current_timestamp()::timestamp_ntz     as stg_load_ts_utc,
    {{ to_nz('current_timestamp()') }}     as stg_load_ts_nz

  from trip_daily td
  left join weather_daily wd
    on td.date_key = wd.date_key
  left join condition_dim cd
    on cd.weather_condition = wd.weather_condition
)

select * from final
