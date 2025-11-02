with raw as (
    select
        CAMPAIGN_ID             as campaign_id,
        CAMPAIGN_NAME           as campaign_name,
        CAMPAIGN_DISCOUNT       as campaign_discount_pct,
        _FIVETRAN_SYNCED        as fivetran_synced_ts
    from {{ source('eco_transactional', 'PROMOTIONAL_CAMPAIGN') }}
)

select * from raw
