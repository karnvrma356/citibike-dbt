{{ config(materialized='incremental', unique_key='trip_sk') }}

with t as (
  select *
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
),

d_user as (
  select user_type_sk, user_type
  from {{ ref('DIM_USER_TYPE') }}
),

d_station as (
  select station_sk, station_id
  from {{ ref('DIM_STATION') }}
)

select
  /* keys */
  t.trip_sk,
  coalesce(u.user_type_sk, '-1') as user_type_sk,
  coalesce(ss.station_sk, '-1')  as start_station_sk,
  coalesce(es.station_sk, '-1')  as end_station_sk,

  /* dates/times */
  t.trip_date,
  t.start_ts,
  t.stop_ts,

  /* measures */
  t.tripduration_sec,
  t.distance_km,

  /* descriptive degenerate attributes (optional but fine) */
  t.bike_id,
  t.birth_year,
  t.gender,

  /* flags */
  t.is_valid,
  t.error_reason,

  /* metadata (END) */
  t.file_name,
  t.src_load_ts_utc,
  t.src_load_ts_nz,
  t.dup_count,
  t.is_duplicate,
  t.dup_rank,
  t.stg_load_ts_utc,
  t.stg_load_ts_nz,
  t.raw_record,
  current_timestamp() as fact_load_ts_utc,
  {{ to_nz('current_timestamp()') }} as fact_load_ts_nz

from t
left join d_user u
  on u.user_type = t.user_type
left join d_station ss
  on ss.station_id = t.start_station_id
left join d_station es
  on es.station_id = t.end_station_id

{% if is_incremental() %}
where t.trip_sk not in (select trip_sk from {{ this }})
{% endif %}
