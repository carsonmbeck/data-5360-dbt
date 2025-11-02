with base as (

    select
        SENDTIMESTAMP          as send_ts,
        EMAILNAME              as email_name,
        EVENTTIMESTAMP         as event_ts,
        SUBSCRIBERID           as subscriber_id,
        CAMPAIGNNAME           as campaign_name,
        SUBSCRIBEREMAIL        as subscriber_email,
        EVENTTYPE              as event_type,
        CUSTOMERID             as customer_id_str,      -- text in source
        SUBSCRIBERFIRSTNAME    as first_name,
        SUBSCRIBERLASTNAME     as last_name,
        CAMPAIGNID             as campaign_id,
        EMAILID                as email_id
    from {{ source('eco_marketing', 'MARKETING_EMAILS') }}

    union all

    select
        SENDTIMESTAMP          as send_ts,
        EMAILNAME              as email_name,
        EVENTTIMESTAMP         as event_ts,
        SUBSCRIBERID           as subscriber_id,
        CAMPAIGNNAME           as campaign_name,
        SUBSCRIBEREMAIL        as subscriber_email,
        EVENTTYPE              as event_type,
        CUSTOMERID             as customer_id_str,
        SUBSCRIBERFIRSTNAME    as first_name,
        SUBSCRIBERLASTNAME     as last_name,
        CAMPAIGNID             as campaign_id,
        EMAILID                as email_id
    from {{ source('eco_marketing', 'SALESFORCE_MARKETING_EMAILS') }}

)

select
    send_ts,
    email_name,
    event_ts,
    subscriber_id,
    campaign_name,
    subscriber_email,
    event_type,
    customer_id_str,
    first_name,
    last_name,
    campaign_id,
    email_id
from base
