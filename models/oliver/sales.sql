{{ config(materialized='table') }}

with f as (
  select *
  from {{ ref('fact_sales') }}
),
d_customer as (
  select
    cust_key,
    firstname,
    lastname,
    email,
    phonenumber,
    state as customer_state
  from {{ ref('oliver_dim_customer') }}
),
d_product as (
  select
    product_key,
    product_name,
    description as product_description
  from {{ ref('oliver_dim_product') }}
),
d_store as (
  select
    store_key,
    store_name,
    street,
    city as store_city,
    state as store_state
  from {{ ref('oliver_dim_store') }}
),
d_employee as (
  select
    employee_key,
    firstname as employee_firstname,
    lastname as employee_lastname,
    position as employee_position
  from {{ ref('oliver_dim_employee') }}
),
d_date as (
  select
    date_key,
    date_id as order_date,
    dayofweek,
    month,
    quarter,
    year
  from {{ ref('oliver_dim_date') }}
)

select
  d_date.order_date,
  d_date.dayofweek,
  d_date.month,
  d_date.quarter,
  d_date.year,

  d_store.store_name,
  d_store.store_city,
  d_store.store_state,

  d_product.product_name,
  d_product.product_description,

  f.quantity,
  f.unit_price,
  f.dollars_sold,
  case when f.quantity > 0 then f.dollars_sold / f.quantity else null end as avg_price,

  d_customer.firstname || ' ' || d_customer.lastname as customer_name,
  d_customer.email as customer_email,
  d_customer.customer_state,

  d_employee.employee_firstname,
  d_employee.employee_lastname,
  d_employee.employee_position

from f
left join d_date     on d_date.date_key = f.date_key
left join d_store    on d_store.store_key = f.store_key
left join d_product  on d_product.product_key = f.product_key
left join d_employee on d_employee.employee_key = f.employee_key
left join d_customer on d_customer.cust_key = f.cust_key
