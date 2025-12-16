{{ config(materialized='view') }}

with base as (
    select *
    from {{ source('cleansed', 'CITIBIKE_TRIPS_CLEAN') }}
),

dedup as (
    select *
    from base
    where dup_rank = 1
),

final as (
    select
        /* =====================================================
           BUSINESS KEYS / IDS (TOP)
        ====================================================== */
        row_hash                        as trip_sk,
        bike_id                         as bike_id,
        start_station_id,
        end_station_id,

        /* =====================================================
           DATES / TIMES
        ====================================================== */
        trip_date,
        start_time                      as start_ts,
        stop_time                       as stop_ts,

        /* =====================================================
           DESCRIPTIVE ATTRIBUTES
        ====================================================== */
        start_station_name,
        end_station_name,

        -- needed for dim_station build
        start_lat,
        start_lng,
        end_lat,
        end_lng,

        upper(coalesce(user_type,'UNKNOWN')) as user_type,
        birth_year,
        gender,

        /* =====================================================
           MEASURES (FACTS)
        ====================================================== */
        tripduration_seconds            as tripduration_sec,

        case
            when start_lat is null or start_lng is null
              or end_lat   is null or end_lng   is null
            then null
            else 2 * 6371 * asin(
                sqrt(
                    pow(sin(radians((end_lat - start_lat) / 2)), 2) +
                    cos(radians(start_lat)) * cos(radians(end_lat)) *
                    pow(sin(radians((end_lng - start_lng) / 2)), 2)
                )
            )
        end as distance_km,

        /* =====================================================
           DATA QUALITY FLAGS
        ====================================================== */
        is_valid,
        error_reason,

        /* =====================================================
           METADATA (ALWAYS AT END)
        ====================================================== */
        file_name,
        load_ts                         as src_load_ts_utc,
        {{ to_nz('load_ts') }}          as src_load_ts_nz,

        dup_count,
        is_duplicate,
        dup_rank,

        current_timestamp()             as stg_load_ts_utc,
        {{ to_nz('current_timestamp()') }} as stg_load_ts_nz,

        raw_record

    from dedup
)

select * from final
