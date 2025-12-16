{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('cleansed', 'CITIBIKE_TRIPS_CLEAN') }}
  where dup_rank = 1
),

typed as (
  select
    /* business keys (TOP) */
    bike_id,
    row_num,

    /* dates/times */
    trip_date,
    start_time,
    stop_time,

    /* descriptive (trip context) */
    start_station_id,
    start_station_name,
    start_lat,
    start_lng,

    end_station_id,
    end_station_name,
    end_lat,
    end_lng,

    user_type,
    birth_year,
    gender,

    /* measures */
    tripduration_seconds                    as tripduration_sec,
    duration_seconds_calc                  as duration_seconds_calc,

    /* haversine distance (km) */
    2 * 6371 * asin(
      sqrt(
        pow(sin(radians((start_lat - end_lat)/2)), 2) +
        cos(radians(end_lat)) * cos(radians(start_lat)) *
        pow(sin(radians((start_lng - end_lng)/2)), 2)
      )
    ) as distance_km,

    /* quality */
    is_valid,
    error_reason,

    /* metadata (END) */
    file_name,
    load_ts                                as src_load_ts_utc,
    {{ to_nz('load_ts') }}                 as src_load_ts_nz,
    current_timestamp()::timestamp_ntz     as stg_load_ts_utc,
    {{ to_nz('current_timestamp()') }}     as stg_load_ts_nz,

    row_hash,
    dup_count,
    is_duplicate,
    dup_rank,

    raw_record
  from src
)

select * from typed
