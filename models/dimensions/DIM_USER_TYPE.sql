{{ config(materialized='table') }}

with src as (

    select
        user_type
    from {{ ref('STG_1_FACT_CITIBIKE_TRIPS') }}
    where dup_rank = 1

),

dedup as (

    select
        -- business key
        coalesce(nullif(trim(user_type), ''), 'UNKNOWN') as user_type
    from src
    qualify row_number() over (
        partition by coalesce(nullif(trim(user_type), ''), 'UNKNOWN')
        order by user_type
    ) = 1

),

final as (

    select
        -- surrogate key must be stable + unique per user_type
        sha2(to_varchar(user_type), 256) as user_type_sk,
        user_type,

        /* metadata at end */
        current_timestamp() as dim_load_ts_utc,
        convert_timezone('UTC', 'Pacific/Auckland', current_timestamp()) as dim_load_ts_nz

    from dedup
)
select *
from final
