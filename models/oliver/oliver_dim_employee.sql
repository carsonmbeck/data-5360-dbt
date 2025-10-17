{{ config(materialized='table') }}

select
    employee_id          as employee_key,
    employee_id          as employeeid,
    first_name           as firstname,
    last_name            as lastname,
    email,
    phone_number         as phone_number,
    hire_date            as hire_date,
    position             as position
from {{ source('OLIVER_DW_SOURCE', 'employee') }}
