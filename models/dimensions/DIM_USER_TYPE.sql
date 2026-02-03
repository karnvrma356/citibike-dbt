{{ config(materialized='table') }}

with src as (
  select distinct
    nullif(trim(user_type::string), '') as user_type_raw
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
),

classified as (
  select
    user_type_raw,
    case
      when user_type_raw is null then 'UNKNOWN'
      when lower(user_type_raw) like '%subscriber%' then 'SUBSCRIBER'
      when lower(user_type_raw) like '%annual%' then 'SUBSCRIBER'
      when lower(user_type_raw) like '%membership%' then 'SUBSCRIBER'
      when lower(user_type_raw) like '%customer%' then 'CASUAL'
      when lower(user_type_raw) like '%day pass%' then 'CASUAL'
      when lower(user_type_raw) like '%single trip%' then 'CASUAL'
      when lower(user_type_raw) like '%24 hour%' then 'CASUAL'
      when lower(user_type_raw) like '%3 day%' then 'CASUAL'
      else 'OTHER'
    end as user_type_norm
  from src
),

dim_real as (
  select
    sha2(coalesce(user_type_raw,'UNKNOWN') || '|' || user_type_norm, 256) as user_type_sk,
    user_type_raw,
    user_type_norm,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
  from classified
),

unknown as (
  select
    sha2('UNKNOWN',256) as user_type_sk,
    null::string as user_type_raw,
    'UNKNOWN'::string as user_type_norm,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
)

select * from unknown
union all
select * from dim_real