{{ config(materialized='table') }}

with base as (
  select
    weather_id,
    weather_main,
    weather_desc,
    weather_icon,
    weather_condition,
    max(src_load_ts_utc) as src_load_ts_utc,
    max(src_load_ts_nz)  as src_load_ts_nz
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
  where weather_id is not null
  group by all
),

ranked as (
  select
    /* keys (TOP) */
    sha2(
      to_varchar(weather_id) || '|' ||
      coalesce(weather_main,'') || '|' ||
      coalesce(weather_desc,''),
      256
    ) as weather_sk,
    weather_id,

    /* descriptions */
    weather_main,
    weather_desc,
    weather_icon,
    weather_condition,

    /* metadata (END) */
    src_load_ts_utc,
    src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz,

    row_number() over (
      partition by
        sha2(
          to_varchar(weather_id) || '|' ||
          coalesce(weather_main,'') || '|' ||
          coalesce(weather_desc,''),
          256
        )
      order by
        src_load_ts_utc desc,
        src_load_ts_nz  desc,
        weather_icon    desc,
        weather_condition desc
    ) as rn
  from base
),

dim as (
  select
    weather_sk,
    weather_id,
    weather_main,
    weather_desc,
    weather_icon,
    weather_condition,
    src_load_ts_utc,
    src_load_ts_nz,
    dim_load_ts_utc,
    dim_load_ts_nz
  from ranked
  qualify rn = 1
),

unknown as (
  select
    sha2('-1',256) as weather_sk,
    -1             as weather_id,
    'Unknown'      as weather_main,
    'Unknown'      as weather_desc,
    null::string   as weather_icon,
    'Unknown'      as weather_condition,

    null::timestamp_ntz                 as src_load_ts_utc,
    null::timestamp_ntz                 as src_load_ts_nz,
    current_timestamp()::timestamp_ntz  as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }}  as dim_load_ts_nz
)

select * from unknown
union all
select * from dim
