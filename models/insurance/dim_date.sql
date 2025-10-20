{{ config(materialized='table', schema='dw_insurance') }}

with dates as (
  select
    dateadd(day, seq4(), to_date('1990-01-01')) as date_day
  from table(generator(rowcount => 22280))   -- literal constant required
)
select
  date_day                            as date_key,
  date_day,
  dayofweek(date_day)                 as day_of_week,
  month(date_day)                     as month_of_year,
  to_char(date_day, 'Month')          as month_name,
  quarter(date_day)                   as quarter_of_year,
  year(date_day)                      as year_number
from dates
