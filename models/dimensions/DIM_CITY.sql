{{ config(materialized='table') }}

with src as (
  select
    city_id,
    city_name,
    city_findname,
    country,
    city_lat,
    city_lon,
    city_zoom,
    src_load_ts_utc,
    src_load_ts_nz
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where city_id is not null
),

ranked as (
  select
    sha2(to_varchar(city_id), 256) as city_sk,
    city_id,
    city_name,
    city_findname,
    country,
    city_lat,
    city_lon,
    city_zoom,
    src_load_ts_utc,
    src_load_ts_nz,
    row_number() over (
      partition by city_id
      order by src_load_ts_utc desc nulls last
    ) as rn
  from src
),

dim as (
  select
    city_sk,
    city_id,
    city_name,
    city_findname,
    country,
    city_lat,
    city_lon,
    city_zoom,
    src_load_ts_utc,
    src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
  from ranked
  qualify rn = 1
),

unknown as (
  select
    sha2('-1',256) as city_sk,
    -1             as city_id,
    'Unknown'      as city_name,
    'Unknown'      as city_findname,
    'Unknown'      as country,
    null::float    as city_lat,
    null::float    as city_lon,
    null::number   as city_zoom,
    null::timestamp_ntz as src_load_ts_utc,
    null::timestamp_ntz as src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
)

select * from unknown
union all
select * from dim