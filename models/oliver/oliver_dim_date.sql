{{ config(materialized='table') }}

{% set start_date = var('date_spine_start', '2019-01-01') %}
{% set end_date   = var('date_spine_end',   '2030-12-31') %}

with spine as (
  select
    dateadd('day', seq4(), to_date('{{ start_date }}')) as date_id
  from table(generator(rowcount => 50000))  
)
select
  to_number(to_char(date_id, 'YYYYMMDD')) as date_key,  
  date_id,                                             
  dayname(date_id)                       as dayofweek,
  trim(to_char(date_id, 'Month'))        as month,
  quarter(date_id)                       as quarter,
  year(date_id)                          as year
from spine
where date_id <= to_date('{{ end_date }}')
