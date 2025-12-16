{{ config(materialized='table') }}

with base as (
  select distinct
    city_id,
    city_name,
    city_findname,
    country,
    city_lat,
    city_lon,
    city_zoom,

    /* metadata */
    max(src_load_ts_utc) as src_load_ts_utc,
    max(src_load_ts_nz)  as src_load_ts_nz
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where city_id is not null
  group by 1,2,3,4,5,6,7
),

dim as (
  select
    /* surrogate + business key (TOP) */
    sha2(to_varchar(city_id), 256) as city_sk,
    city_id,

    /* descriptions */
    city_name,
    city_findname,
    country,
    city_lat,
    city_lon,
    city_zoom,

    /* metadata (END) */
    src_load_ts_utc,
    src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
  from base
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

    current_timestamp()::timestamp_ntz as src_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
)

select * from unknown
union all
select * from dim
