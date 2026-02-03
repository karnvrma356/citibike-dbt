{{ config(materialized='table') }}

with dim_real as (
  select
    station_sk::string as station_sk,
    station_id::number(38,0) as station_id,
    station_name::string as station_name,
    lat::float as lat,
    lon::float as lon,
    src_load_ts_utc,
    src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
  from {{ ref('STG_1_DIM_STATION') }}
),

unknown as (
  select
    sha2('UNKNOWN', 256)::string as station_sk,
    -1::number(38,0)             as station_id,
    'UNKNOWN'::string            as station_name,
    null::float                  as lat,
    null::float                  as lon,
    null::timestamp_ntz          as src_load_ts_utc,
    null::timestamp_ntz          as src_load_ts_nz,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
)

select * from unknown
union all
select * from dim_real