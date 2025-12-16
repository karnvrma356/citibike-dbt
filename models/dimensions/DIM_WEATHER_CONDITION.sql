{{ config(materialized='table') }}

with base as (
  select distinct
    upper(coalesce(weather_condition,'UNKNOWN')) as weather_condition
  from {{ ref('STG_1_DIM_WEATHER_NYC') }}
),

dim as (
  select
    /* keys (TOP) */
    sha2(weather_condition, 256) as weather_condition_sk,
    weather_condition,

    /* metadata (END) */
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
  from base
),

unknown as (
  select
    sha2('UNKNOWN',256) as weather_condition_sk,
    'UNKNOWN'::string   as weather_condition,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
)

select * from unknown
union all
select * from dim
