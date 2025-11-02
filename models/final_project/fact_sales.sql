{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

with line_items as (
    select
        ol.order_line_id,
        ol.order_id,
        ol.product_id,
        ol.quantity,
        ol.line_price_after_discount as sales_amount
    from {{ ref('stg_order_line') }} ol
),

orders as (
    select
        o.order_id,
        o.customer_id,
        o.order_timestamp::date as order_date
    from {{ ref('stg_order') }} o
),

joined as (
    select
        li.order_line_id,
        li.order_id,
        li.product_id,
        o.customer_id,
        o.order_date,
        li.quantity,
        li.sales_amount
    from line_items li
    join orders o
      on li.order_id = o.order_id
)

select
    dp.product_key,
    dc.customer_key,
    dd.date_key,
    dord.order_key,

    -- measures
    j.sales_amount        as SalesAmount,
    j.quantity            as QuantitySold

from joined j
join {{ ref('dim_product') }} dp
  on j.product_id = dp.product_id
join {{ ref('dim_customer') }} dc
  on j.customer_id = dc.customer_id
join {{ ref('dim_order') }} dord
  on j.order_id = dord.order_id
join {{ ref('dim_date') }} dd
  on j.order_date = dd.full_date

