{{ config(materialized='table') }}

with orders as (
    select
        order_id,
        customer_id,
        employee_id,
        store_id,
        cast(order_date as date) as order_date
    from {{ source('OLIVER_DW_SOURCE', 'orders') }}
),
orderline as (
    select
        order_id,
        product_id,
        quantity,
        unit_price
    from {{ source('OLIVER_DW_SOURCE', 'orderline') }}
),
dim_customer as (
    select cust_key, customerid
    from {{ ref('oliver_dim_customer') }}
),
dim_product as (
    select product_key, product_id
    from {{ ref('oliver_dim_product') }}
),
dim_store as (
    select store_key, store_id
    from {{ ref('oliver_dim_store') }}
),
dim_employee as (
    select employee_key, employeeid
    from {{ ref('oliver_dim_employee') }}
),
dim_date as (
    select date_key, date_id
    from {{ ref('oliver_dim_date') }}
),
prod_price as (
    select product_id, unit_price as product_unit_price
    from {{ source('OLIVER_DW_SOURCE', 'product') }}
)

select
    dc.cust_key,
    dd.date_key,
    ds.store_key,
    dp.product_key,
    de.employee_key,
    ol.quantity,
    coalesce(ol.unit_price, pp.product_unit_price) as unit_price,
    (ol.quantity * coalesce(ol.unit_price, pp.product_unit_price))::number(18,2) as dollars_sold
from orderline ol
join orders o on o.order_id = ol.order_id
left join dim_customer dc on dc.customerid = o.customer_id
left join dim_store ds on ds.store_id = o.store_id
left join dim_employee de on de.employeeid = o.employee_id
left join dim_product dp on dp.product_id = ol.product_id
left join dim_date dd on dd.date_id = o.order_date
left join prod_price pp on pp.product_id = ol.product_id
