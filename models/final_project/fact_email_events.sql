{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

with events as (
    select
        m.send_ts::date        as send_date,
        m.send_ts              as send_ts,
        m.event_ts             as event_ts,
        m.event_type,
        m.email_id,
        m.email_name,
        try_to_number(m.customer_id_str) as customer_id,
        m.campaign_id
    from {{ ref('stg_marketing_emails') }} m
),

enriched as (
    select
        e.send_date,
        e.send_ts,
        e.event_ts,
        e.event_type,
        e.email_id,
        e.customer_id,
        e.campaign_id,

        dc.customer_key              as customer_key,
        dce.email_key                as email_key,
        det.email_event_key          as email_event_key,
        dcam.campaign_key            as campaign_key,
        dd.date_key                  as date_key,

        1 as event_count,
        coalesce(dcam.campaign_discount, 0) as discount_offer_each
    from events e
    left join {{ ref('dim_customer') }} dc
      on e.customer_id = dc.customer_id
    left join {{ ref('dim_campaign_email') }} dce
      on e.email_id = dce.email_id
    left join {{ ref('dim_event_type') }} det
      on e.event_type = det.event_type
     and e.event_ts   = det.event_timestamp
    left join {{ ref('dim_campaign') }} dcam
      on e.campaign_id = dcam.campaign_id
    left join {{ ref('dim_date') }} dd
      on e.send_date = dd.full_date
)

select
    enriched.email_event_key,
    enriched.email_key,
    enriched.customer_key,
    enriched.date_key,
    enriched.campaign_key,

    sum(enriched.event_count)              as count,
    sum(enriched.discount_offer_each)      as total_discount_offered
from enriched
group by
    enriched.email_event_key,
    enriched.email_key,
    enriched.customer_key,
    enriched.date_key,
    enriched.campaign_key

