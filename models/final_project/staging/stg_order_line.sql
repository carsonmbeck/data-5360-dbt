with raw as (
    select
        ORDER_LINE_ID           as order_line_id,
        ORDER_ID                as order_id,
        PRODUCT_ID              as product_id,
        CAMPAIGN_ID             as campaign_id,
        QUANTITY                as quantity,
        DISCOUNT                as discount_pct,
        PROMOTIONAL_CAMPAIGN    as promotional_campaign_flag,
        PRICE_AFTER_DISCOUNT    as line_price_after_discount,
        _FIVETRAN_SYNCED        as fivetran_synced_ts
    from {{ source('eco_transactional', 'ORDER_LINE') }}
)

select * from raw
