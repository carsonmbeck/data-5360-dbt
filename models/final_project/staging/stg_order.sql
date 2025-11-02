with raw as (
    select
        ORDER_ID            as order_id,
        CUSTOMER_ID         as customer_id,
        ORDER_TIMESTAMP     as order_timestamp,
        _FIVETRAN_SYNCED    as fivetran_synced_ts
    from {{ source('eco_transactional', 'ORDERS') }}
)

select * from raw
