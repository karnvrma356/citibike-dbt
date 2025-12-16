{{ config(materialized='table', schema='MART') }}

with base as (
  select distinct
    weather_id,
    weather_main,
    weather_desc,
    weather_icon,

    /* metadata */
    max(src_load_ts_utc) as src_load_ts_utc,
    max(src_load_ts_nz)  as src_load_ts_nz
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where weather_id is not null
  group by 1,2,3,4
),

dim as (
  select
    /* surrogate + business key (TOP) */
    sha2(to_varchar(weather_id) || '|' || coalesce(weather_main,'') || '|' || coalesce(weather_desc,''), 256) as weather_sk,
    weather_id,

    /* descriptions */
    weather_main,
    weather_desc,
    weather_icon,

    /* metadata (END) */
    src_load_ts_utc,
    src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
  from base
),

unknown as (
  select
    sha2('-1',256) as weather_sk,
    -1             as weather_id,
    'Unknown'      as weather_main,
    'Unknown'      as weather_desc,
    null::string   as weather_icon,

    current_timestamp()::timestamp_ntz as src_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
)

select * from unknown
union all
select * from dim
