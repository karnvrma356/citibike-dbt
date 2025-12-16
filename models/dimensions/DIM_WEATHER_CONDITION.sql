{{ config(materialized='table') }}

select
  /* keys */
  '-1'::string as weather_condition_sk,

  /* descriptions */
  'UNKNOWN'::string as weather_condition,

  /* metadata (END) */
  current_timestamp() as dim_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as dim_load_ts_nz

union all

select distinct
  /* keys */
  sha2(weather_condition, 256)::string as weather_condition_sk,

  /* descriptions */
  weather_condition,

  /* metadata (END) */
  current_timestamp() as dim_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as dim_load_ts_nz

from {{ ref('STG_1_DIM_WEATHER_NYC') }}
