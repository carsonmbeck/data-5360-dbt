{{ config(materialized='table') }}

select
    store_id                          as store_key,
    store_id                          as store_id,
    store_name                        as store_name,
    street                            as street,
    city                              as city,
    state                             as state

from {{ source('OLIVER_DW_SOURCE', 'store') }}
