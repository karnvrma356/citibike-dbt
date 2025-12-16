{{ config(materialized='view') }}

with trips as (
  select *
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
),

stations_union as (
  select
    start_station_id        as station_id,
    start_station_name      as station_name,
    start_lat::float        as lat,
    start_lng::float        as lon,
    src_load_ts_utc,
    src_load_ts_nz
  from trips
  where start_station_id is not null

  union all

  select
    end_station_id          as station_id,
    end_station_name        as station_name,
    end_lat::float          as lat,
    end_lng::float          as lon,
    src_load_ts_utc,
    src_load_ts_nz
  from trips
  where end_station_id is not null
),

rolled as (
  select
    station_id,
    max_by(station_name, src_load_ts_utc) as station_name,
    max_by(lat, src_load_ts_utc)          as lat,
    max_by(lon, src_load_ts_utc)          as lon,
    max(src_load_ts_utc)                  as src_load_ts_utc,
    max(src_load_ts_nz)                   as src_load_ts_nz
  from stations_union
  group by station_id
)

select
  /* keys */
  sha2(to_varchar(station_id), 256)       as station_sk,
  station_id,

  /* descriptions */
  station_name,
  lat,
  lon,

  /* metadata (END) */
  src_load_ts_utc,
  src_load_ts_nz,
  current_timestamp()                    as stg_load_ts_utc,
  {{ to_nz('current_timestamp()') }}     as stg_load_ts_nz

from rolled

