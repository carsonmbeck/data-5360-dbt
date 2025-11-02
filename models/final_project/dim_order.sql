{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

with orders as (
    select
        o.order_id,
        o.customer_id,
        o.order_timestamp
    from {{ ref('stg_order') }} o
),

qty_per_order as (
    select
        ol.order_id,
        sum(ol.quantity) as total_quantity
    from {{ ref('stg_order_line') }} ol
    group by ol.order_id
)

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
    o.order_id,
    o.order_timestamp,
    q.total_quantity as quantity
from orders o
left join qty_per_order q
  on o.order_id = q.order_id
