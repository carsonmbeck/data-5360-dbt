with raw as (
    select
        PRODUCT_ID          as product_id,
        PRODUCT_TYPE        as product_type,
        PRODUCT_NAME        as product_name,
        PRICE               as list_price,
        _FIVETRAN_SYNCED    as fivetran_synced_ts
    from {{ source('eco_transactional', 'PRODUCT') }}
)

select * from raw
