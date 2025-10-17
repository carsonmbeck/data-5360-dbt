{{ config(materialized='table') }}

select
    product_id                     as product_key,
    product_id                     as product_id,
    product_name                   as product_name,
    description                    as description,
    unit_price                     as unit_price

from {{ source('OLIVER_DW_SOURCE', 'product') }}
