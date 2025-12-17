{{ config(materialized='table') }}

with base as (
    select
        station_id,
        station_name,
        lat,
        lon,
        src_load_ts_utc,
        src_load_ts_nz
    from {{ ref('STG_1_DIM_STATION') }}
    where station_id is not null
),

dedup as (
    select
        station_id,
        -- take the latest attributes by source load timestamp
        max_by(station_name, src_load_ts_utc) as station_name,
        max_by(lat,          src_load_ts_utc) as lat,
        max_by(lon,          src_load_ts_utc) as lon,
        max(src_load_ts_utc)                  as src_load_ts_utc,
        max(src_load_ts_nz)                   as src_load_ts_nz
    from base
    group by station_id
),

final as (

    /* UNKNOWN row (keep types consistent: station_sk STRING, station_id NUMBER) */
    select
        sha2('UNKNOWN', 256)::string          as station_sk,
        -1::number                            as station_id,
        'UNKNOWN'::string                     as station_name,
        null::float                           as lat,
        null::float                           as lon,
        null::timestamp_ntz                   as src_load_ts_utc,
        null::timestamp_ntz                   as src_load_ts_nz,
        current_timestamp()                   as dim_load_ts_utc,
        {{ to_nz('current_timestamp()') }}    as dim_load_ts_nz

    union all

    /* REAL stations */
    select
        sha2(to_varchar(station_id), 256)::string as station_sk,
        station_id::number                        as station_id,
        station_name::string                      as station_name,
        lat::float                                as lat,
        lon::float                                as lon,
        src_load_ts_utc,
        src_load_ts_nz,
        current_timestamp()                       as dim_load_ts_utc,
        {{ to_nz('current_timestamp()') }}        as dim_load_ts_nz
    from dedup
)

select *
from final