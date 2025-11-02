{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['et.event_type','et.event_ts']) }} as email_event_key,
    et.event_type,
    et.event_ts as event_timestamp
from (
    select distinct
        event_type,
        event_ts
    from {{ ref('stg_marketing_emails') }}
    where event_type is not null
      and event_ts   is not null
) et
