{{ config(materialized='view') }}

with trips as (
    select *
    from {{ source('cleansed', 'CITIBIKE_TRIPS_CLEAN') }}
    where dup_rank = 1
      and is_valid = true
),

stations as (

    /* START stations */
    select
        start_station_id   as station_id,
        start_station_name as station_name,
        start_lat          as lat,
        start_lng          as lon,
        load_ts            as src_load_ts_utc
    from trips
    where start_station_id is not null

    union all

    /* END stations */
    select
        end_station_id     as station_id,
        end_station_name   as station_name,
        end_lat            as lat,
        end_lng            as lon,
        load_ts            as src_load_ts_utc
    from trips
    where end_station_id is not null
),

deduped as (
    select
        station_id,
        max_by(station_name, src_load_ts_utc) as station_name,
        max_by(lat, src_load_ts_utc)          as lat,
        max_by(lon, src_load_ts_utc)          as lon,
        max(src_load_ts_utc)                  as src_load_ts_utc
    from stations
    group by station_id
)

select
    /* business key */
    station_id,

    /* descriptions */
    station_name,
    lat,
    lon,

    /* metadata (END) */
    src_load_ts_utc,
    {{ to_nz('src_load_ts_utc') }}            as src_load_ts_nz,
    current_timestamp()                      as stg_load_ts_utc,
    {{ to_nz('current_timestamp()') }}       as stg_load_ts_nz

from deduped
