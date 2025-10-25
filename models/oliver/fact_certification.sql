{{ config(materialized='table', schema='DW_OLIVER') }}

with src as (
    select
        employee_id,
        certification_name,
        certification_cost,
        award_date_key
    from {{ ref('stg_employee_certifications') }}
),

fact_rows as (
    select
        e.employee_key,
        d.date_key,
        s.certification_name,
        s.certification_cost
    from src s
    inner join {{ ref('oliver_dim_employee') }} e
        on e.employeeid = s.employee_id              
    inner join {{ ref('oliver_dim_date') }} d
        on d.date_key = s.award_date_key
)

select * from fact_rows
