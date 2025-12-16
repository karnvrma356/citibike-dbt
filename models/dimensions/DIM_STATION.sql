{{ config(materialized='table') }}

select
  /* keys */
  '-1'::string                 as station_sk,
  null::number                 as station_id,

  /* descriptions */
  'UNKNOWN'::string            as station_name,
  null::float                  as lat,
  null::float                  as lon,

  /* metadata (END) */
  null::timestamp_ntz          as src_load_ts_utc,
  null::timestamp_ntz          as src_load_ts_nz,
  current_timestamp()          as dim_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as dim_load_ts_nz

union all

select
  /* keys */
  station_sk::string,
  station_id,

  /* descriptions */
  station_name,
  lat,
  lon,

  /* metadata (END) */
  src_load_ts_utc,
  src_load_ts_nz,
  current_timestamp()          as dim_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as dim_load_ts_nz

from {{ ref('STG_1_DIM_STATION') }}
