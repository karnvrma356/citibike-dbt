{{ config(materialized='table') }}

with s as (
  select *
  from {{ ref('STG_1_FACT_TRIPS_DAILY') }}
),

dim_w as (
  select weather_id, weather_sk
  from {{ ref('DIM_WEATHER') }}
),

final as (
  select
    /* keys (TOP) */
    s.date_key,
    coalesce(dw.weather_sk, sha2('-1',256)) as weather_sk,
    

    /* dates */
    s.trip_date,

    /* measures */
    s.trip_count,
    s.avg_duration_sec,
    s.avg_distance_km,
    s.total_distance_km,
    s.avg_temp_k,
    s.avg_humidity_pct,
    s.avg_cloud_pct,
    s.avg_wind_speed,

    /* metadata (END) */
    s.src_load_ts_utc,
    s.src_load_ts_nz,
    current_timestamp()::timestamp_ntz as fact_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as fact_load_ts_nz
  from s
  left join dim_w dw
    on s.weather_id = dw.weather_id
)

select * from final
