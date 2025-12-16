{{ config(materialized='table') }}

select
  /* keys */
  '-1'::string as user_type_sk,

  /* descriptions */
  'UNKNOWN'::string as user_type,

  /* metadata (END) */
  current_timestamp() as dim_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as dim_load_ts_nz

union all

select distinct
  /* keys */
  sha2(user_type, 256)::string as user_type_sk,

  /* descriptions */
  user_type,

  /* metadata (END) */
  current_timestamp() as dim_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as dim_load_ts_nz

from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
