{{ config(materialized='table') }}

with base as (
  select distinct
    upper(coalesce(user_type,'UNKNOWN')) as user_type
  from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
),

dim as (
  select
    sha2(user_type,256) as user_type_sk,
    user_type,

    /* metadata (END) */
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
  from base
),

unknown as (
  select
    sha2('UNKNOWN',256) as user_type_sk,
    'UNKNOWN'::string   as user_type,
    current_timestamp()::timestamp_ntz as dim_load_ts_utc,
    {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
)

select * from unknown
union all
select * from dim
