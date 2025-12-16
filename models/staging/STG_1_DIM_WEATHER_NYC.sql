{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('cleansed', 'WEATHER_NYC_CLEAN') }}
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

    /* grouped condition (Option A) */
    case
      when lower(weather_main) like '%rain%' or lower(weather_desc) like '%rain%' or lower(weather_desc) like '%drizzle%' then 'Rain'
      when lower(weather_main) like '%snow%' or lower(weather_desc) like '%snow%' then 'Snow'
      when lower(weather_main) like '%cloud%' or lower(weather_desc) like '%cloud%' then 'Clouds'
      else 'Clear'
    end as weather_condition,

    /* numeric weather (already clean columns) */
    temp_k,
    temp_min_k,
    temp_max_k,
    humidity_pct,
    pressure_hpa,
    cloud_pct,
    wind_speed,
    wind_deg,

    /* langs (optional) */
    lang_index,
    lang_abbr,
    lang_link,
    lang_value,

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

select * from final
