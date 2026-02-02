{{ config(materialized='table') }}

with src as (
    select
        weather_id,
        weather_main,
        weather_desc,
        weather_icon,
        weather_condition,
        src_load_ts_utc,
        src_load_ts_nz
    from {{ ref('STG_1_DIM_WEATHER_NYC') }}
    where weather_id is not null
      and coalesce(weather_main, weather_desc) is not null
),

/* If the same weather_id/main/desc appears many times (different timestamps),
   keep the newest record as the canonical representation. */
ranked as (
    select
        sha2(
            concat_ws('|',
                to_varchar(weather_id),
                coalesce(weather_main, ''),
                coalesce(weather_desc, '')
            ),
            256
        ) as weather_sk,

        weather_id,
        weather_main,
        weather_desc,

        /* icon / condition can vary; pick latest */
        weather_icon,
        weather_condition,

        src_load_ts_utc,
        src_load_ts_nz,

        row_number() over (
            partition by
                weather_id,
                coalesce(weather_main, ''),
                coalesce(weather_desc, '')
            order by
                src_load_ts_utc desc nulls last,
                src_load_ts_nz  desc nulls last,
                weather_icon    desc nulls last,
                weather_condition desc nulls last
        ) as rn
    from src
),

dim as (
    select
        weather_sk,
        weather_id,
        weather_main,
        weather_desc,
        weather_icon,
        weather_condition,

        /* keep latest seen timestamps for lineage */
        src_load_ts_utc,
        src_load_ts_nz,

        current_timestamp()::timestamp_ntz as dim_load_ts_utc,
        {{ to_nz('current_timestamp()') }} as dim_load_ts_nz
    from ranked
    qualify rn = 1
),

unknown as (
    select
        sha2('-1',256) as weather_sk,
        -1             as weather_id,
        'Unknown'      as weather_main,
        'Unknown'      as weather_desc,
        null::string   as weather_icon,
        'Unknown'      as weather_condition,
        null::timestamp_ntz                 as src_load_ts_utc,
        null::timestamp_ntz                 as src_load_ts_nz,
        current_timestamp()::timestamp_ntz  as dim_load_ts_utc,
        {{ to_nz('current_timestamp()') }}  as dim_load_ts_nz
)

select * from unknown
union all
select * from dim
