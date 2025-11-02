{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['e.email_id']) }} as email_key,
    e.email_id,
    e.email_name
from (
    select
        email_id,
        email_name,
        event_ts,
        row_number() over (
            partition by email_id
            order by event_ts desc
        ) as rn
    from {{ ref('stg_marketing_emails') }}
    where email_id is not null
) e
where e.rn = 1
