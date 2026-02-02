{{ config(materialized='table') }}

with trips as (
    select
        start_station_id,
        start_station_name,
        start_lat,
        start_lng,

        end_station_id,
        end_station_name,
        end_lat,
        end_lng,

        load_ts as src_load_ts_utc
    from {{ source('cleansed', 'CITIBIKE_TRIPS_CLEAN') }}
    where dup_rank = 1
      and is_valid = true
),

stations_union as (
    /* START stations */
    select
        nullif(trim(start_station_id::string), '') as station_id,
        nullif(trim(regexp_replace(start_station_name::string, '[[:cntrl:]]', '')), '') as station_name,
        try_to_double(start_lat) as lat,
        try_to_double(start_lng) as lon,
        src_load_ts_utc
    from trips

    union all

    /* END stations */
    select
        nullif(trim(end_station_id::string), '') as station_id,
        nullif(trim(regexp_replace(end_station_name::string, '[[:cntrl:]]', '')), '') as station_name,
        try_to_double(end_lat) as lat,
        try_to_double(end_lng) as lon,
        src_load_ts_utc
    from trips
),

filtered as (
    /* drop unusable rows */
    select *
    from stations_union
    where station_id is not null
      and (station_name is not null or lat is not null or lon is not null)
),

/* choose the newest record per station_id, but prefer rows with richer data */
ranked as (
    select
        station_id,
        station_name,
        lat,
        lon,
        src_load_ts_utc,

        row_number() over (
            partition by station_id
            order by
                /* prefer rows that have name+coords */
                iff(station_name is not null, 1, 0) desc,
                iff(lat is not null and lon is not null, 1, 0) desc,
                src_load_ts_utc desc
        ) as rn,

        max(src_load_ts_utc) over (partition by station_id) as max_src_load_ts_utc
    from filtered
),

final as (
    select
        /* keys */
        sha2(station_id, 256) as station_sk,
        station_id,

        /* descriptions */
        station_name,
        lat,
        lon,

        /* metadata */
        max_src_load_ts_utc as src_load_ts_utc,
        {{ to_nz('max_src_load_ts_utc') }} as src_load_ts_nz,
        current_timestamp()::timestamp_ntz as stg_load_ts_utc,
        {{ to_nz('current_timestamp()') }} as stg_load_ts_nz
    from ranked
    qualify rn = 1
)

select * from final
