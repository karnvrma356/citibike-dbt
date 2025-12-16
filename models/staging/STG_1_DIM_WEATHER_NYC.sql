{{ config(materialized='view', schema='STAGE') }}

with src as (
  select *
  from CLEANSED_CITIBIKE.CORE.WEATHER_NYC_CLEAN
  where dup_rank = 1
),

final as (
  select
    /* business keys (TOP) */
    city_id,
    weather_id,

    /* dates */
    weather_date,
    time_num,

    /* city descriptive */
    city_name,
    city_findname,
    country,
    city_lat,
    city_lon,
    city_zoom,

    /* weather descriptive */
    weather_main,
    weather_desc,
    weather_icon,

    /* optional lang fields (already flattened in your CLEANSED DT) */
    lang_index,
    lang_abbr,
    lang_link,
    lang_value,

    /* quality flags */
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

select * from final
