{{ config(materialized='table') }}

with src as (
  select *
  from {{ source('cleansed', 'WEATHER_NYC_CLEAN') }}
  where is_valid = true
    and dup_rank = 1
    --and coalesce(is_duplicate,false) = false
),

normalized as (
  select
    city_id,
    weather_id,

    case
      when time_num is null then null
      when time_num >= 1000000000000 then to_timestamp_ntz(time_num / 1000)
      else to_timestamp_ntz(time_num)
    end as weather_datetime,

    coalesce(
      to_date(
        case
          when time_num is null then null
          when time_num >= 1000000000000 then to_timestamp_ntz(time_num / 1000)
          else to_timestamp_ntz(time_num)
        end
      ),
      weather_date
    ) as weather_date,

    time_num,

    city_name,
    city_findname,
    country,
    city_lat,
    city_lon,
    city_zoom,

    weather_main,
    weather_desc,
    weather_icon,

    case
      when lower(weather_main) like '%rain%' or lower(weather_desc) like '%rain%' or lower(weather_desc) like '%drizzle%' then 'Rain'
      when lower(weather_main) like '%snow%' or lower(weather_desc) like '%snow%' then 'Snow'
      when lower(weather_main) like '%cloud%' or lower(weather_desc) like '%cloud%' then 'Clouds'
      else 'Clear'
    end as weather_condition,

    temp_k,
    temp_min_k,
    temp_max_k,
    humidity_pct,
    pressure_hpa,
    cloud_pct,
    wind_speed,
    wind_deg,

    error_reason,
    file_name,
    load_ts as src_load_ts_utc,
    {{ to_nz('load_ts') }} as src_load_ts_nz,
    current_timestamp()::timestamp_ntz as stg_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as stg_load_ts_nz,

    row_hash,
    dup_count,
    is_duplicate,
    dup_rank,
    raw_record
  from src
),

dedup as (
  select *
  from normalized
  where weather_datetime is not null
  qualify row_number() over (
    partition by city_id, weather_datetime
    order by src_load_ts_utc desc, file_name desc
  ) = 1
)

select
  sha2(
    concat_ws('|',
      to_varchar(city_id),
      to_varchar(weather_datetime),
      to_varchar(weather_id),
      coalesce(weather_main,''),
      coalesce(weather_desc,'')
    ),
    256
  ) as dim_weather_sk,

  city_id,
  weather_id,
  weather_datetime,
  weather_date,
  date_part('hour', weather_datetime) as weather_hour,
  date_part('minute', weather_datetime) as weather_minute,

  city_name,
  city_findname,
  country,
  city_lat,
  city_lon,
  city_zoom,

  weather_main,
  weather_desc,
  weather_icon,
  weather_condition,

  temp_k,
  temp_min_k,
  temp_max_k,
  humidity_pct,
  pressure_hpa,
  cloud_pct,
  wind_speed,
  wind_deg,

  time_num,

  error_reason,
  file_name,
  src_load_ts_utc,
  src_load_ts_nz,
  stg_load_ts_utc,
  stg_load_ts_nz,

  row_hash,
  dup_count,
  is_duplicate,
  dup_rank,
  raw_record
from dedup