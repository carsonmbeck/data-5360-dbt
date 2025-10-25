{{ config(materialized='table', schema='OLIVER_DW_SOURCE') }}  

with src as (
    select
        employee_id,
        first_name,
        last_name,
        email,
        certification_json
    from {{ source('OLIVER_DW_SOURCE','employee_certifications') }}
),

parsed as (
    select
        employee_id,

        first_name,
        last_name,
        email,

        parse_json(certification_json):certification_name::string        as certification_name,
        parse_json(certification_json):certification_cost::number(10,2)  as certification_cost,

        try_to_date(parse_json(certification_json):certification_awarded_date::string) as certification_awarded_date,

        to_number(to_char(
            try_to_date(parse_json(certification_json):certification_awarded_date::string),
            'YYYYMMDD'
        )) as award_date_key
    from src
)

select * from parsed
