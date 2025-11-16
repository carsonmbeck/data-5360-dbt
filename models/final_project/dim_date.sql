{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

with date_spine as (
    select
        dateadd(day, seq4(), to_date('2022-01-01')) as full_date
    from table(generator(rowcount => 2000))  -- ~2000 days (~5.5 years)
),

final as (
    select
        full_date,
        to_char(full_date, 'YYYYMMDD')::number as date_key,
        extract(day        from full_date) as day,
        extract(month      from full_date) as month,
        extract(quarter    from full_date) as quarter,
        extract(year       from full_date) as year,
        extract(week       from full_date) as week_of_year
    from date_spine
)

select
    date_key,
    full_date,
    day,
    month,
    quarter,
    year,
    week_of_year
from final
