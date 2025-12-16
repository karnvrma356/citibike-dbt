{{ config(
    materialized='incremental',
    unique_key='date_key'
) }}

{% set start_date = '1990-01-01' %}
{% set end_date   = '2026-12-31' %}

with dates as (

    -- Snowflake GENERATOR requires constant ROWCOUNT.
    -- 15,000 days is ~41 years, enough for 1990 -> 2026 (plus buffer).
    select
        dateadd(day, seq4(), to_date('{{ start_date }}'))::date as date_day
    from table(generator(rowcount => 20000))

),

filtered as (
    select
        date_day
    from dates
    where date_day <= to_date('{{ end_date }}')
),

enriched as (
    select
        date_day,
        to_number(to_char(date_day, 'YYYYMMDD')) as date_key,

        -- Calendar attributes
        year(date_day) as year_num,
        quarter(date_day) as quarter_num,
        month(date_day) as month_num,
        to_char(date_day, 'MMMM') as month_name,
        day(date_day) as day_of_month,

        dayofweekiso(date_day) as day_of_week_iso,          -- 1=Mon..7=Sun
        to_char(date_day, 'DY') as day_name_short,
        rtrim(to_char(date_day, 'DAY')) as day_name,

        weekofyear(date_day) as week_of_year,
        date_trunc('week', date_day)::date as week_start_date,
        dateadd(day, 6, date_trunc('week', date_day))::date as week_end_date,

        iff(dayofweekiso(date_day) in (6,7), true, false) as is_weekend,

        -- Fiscal (FY starts April = month 4)
        iff(month(date_day) >= 4, year(date_day) + 1, year(date_day)) as fiscal_year,
        mod(month(date_day) - 4 + 12, 12) + 1 as fiscal_month_num,
        ceil((mod(month(date_day) - 4 + 12, 12) + 1) / 3) as fiscal_quarter,

        to_char(
          dateadd(month, mod(month(date_day) - 4 + 12, 12), date_from_parts(2000, 4, 1)),
          'MMMM'
        ) as fiscal_month_name,

        -- Flags
        iff(date_day = date_trunc('month', date_day), true, false) as is_month_start,
        iff(date_day = last_day(date_day), true, false) as is_month_end,

        iff(date_day = date_trunc('quarter', date_day), true, false) as is_quarter_start,
        iff(date_day = last_day(date_trunc('quarter', date_day), 'quarter'), true, false) as is_quarter_end,

        iff(date_day = date_trunc('year', date_day), true, false) as is_year_start,
        iff(date_day = last_day(date_day, 'year'), true, false) as is_year_end,

        iff(month(date_day) = 4 and day(date_day) = 1, true, false) as is_fiscal_year_start,
        iff(
          dateadd(
            day, -1,
            date_from_parts(
              iff(month(date_day) >= 4, year(date_day) + 1, year(date_day)),
              4,
              1
            )
          ) = date_day,
          true, false
        ) as is_fiscal_year_end,

        current_timestamp() as dw_load_ts
    from filtered
)

select *
from enriched

{% if is_incremental() %}
where date_day > (select coalesce(max(date_day), to_date('1900-01-01')) from {{ this }})
{% endif %}
