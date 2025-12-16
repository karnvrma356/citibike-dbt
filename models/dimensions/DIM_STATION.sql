{{ config(
    materialized='table'
) }}

with base as (
    select *
    from {{ ref('STG_1_DIM_STATION') }}
),

final as (

    /* UNKNOWN row */
    select
        '-1'                                as station_sk,
        -1                                  as station_id,
        'UNKNOWN'                           as station_name,
        null::float                         as lat,
        null::float                         as lon,
        null::timestamp_ntz                 as src_load_ts_utc,
        null::timestamp_ntz                 as src_load_ts_nz,
        current_timestamp()                 as dim_load_ts_utc,
        {{ to_nz('current_timestamp()') }}  as dim_load_ts_nz

    union all

    /* REAL stations */
    select
        sha2(to_varchar(station_id), 256)   as station_sk,
        station_id,
        station_name,
        lat,
        lon,
        src_load_ts_utc,
        src_load_ts_nz,
        current_timestamp()                 as dim_load_ts_utc,
        {{ to_nz('current_timestamp()') }}  as dim_load_ts_nz
    from base
)

select *
from final