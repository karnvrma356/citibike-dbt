{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('cleansed', 'CITIBIKE_TRIPS_CLEAN') }}
  where dup_rank = 1
    and is_valid = true
    and coalesce(is_duplicate,false) = false
),

typed as (
  select
    /* stable trip key (critical for bridges/facts) */
    sha2(row_hash, 256) as trip_sk,
    row_hash            as trip_row_hash,

    /* core fields */
    bike_id,
    row_num,
    trip_date,
    start_time,
    stop_time,

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

    tripduration_seconds::number(38,0)  as tripduration_sec,
    duration_seconds_calc::number(18,0) as duration_seconds_calc,

    /* correct haversine (km) */
    2 * 6371 * asin(
      sqrt(
        pow(sin(radians((end_lat - start_lat) / 2)), 2) +
        cos(radians(start_lat)) * cos(radians(end_lat)) *
        pow(sin(radians((end_lng - start_lng) / 2)), 2)
      )
    ) as distance_km,

    error_reason,

    file_name,
    load_ts                                as src_load_ts_utc,
    {{ to_nz('load_ts') }}                 as src_load_ts_nz,
    current_timestamp()::timestamp_ntz     as stg_load_ts_utc,
    {{ to_nz('current_timestamp()') }}     as stg_load_ts_nz,

    raw_record
  from src
)

select * from typed