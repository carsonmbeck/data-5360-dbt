{{ config(materialized='table') }}

select
    customer_id                                   as cust_key,
    customer_id                                   as customerid,
    first_name                                    as firstname,
    last_name                                     as lastname,
    email,
    phone_number                                  as phonenumber,
    state

from {{ source('OLIVER_DW_SOURCE', 'customer') }}
