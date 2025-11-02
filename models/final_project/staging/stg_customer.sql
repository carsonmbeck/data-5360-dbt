with raw as (
    select
        CUSTOMER_ID            as customer_id,
        CUSTOMER_FIRST_NAME    as first_name,
        CUSTOMER_LAST_NAME     as last_name,
        CUSTOMER_PHONE         as phone,
        CUSTOMER_ADDRESS       as address,
        CUSTOMER_CITY          as city,
        CUSTOMER_STATE         as state,
        CUSTOMER_ZIP           as zip,
        CUSTOMER_COUNTRY       as country,
        _FIVETRAN_SYNCED       as fivetran_synced_ts
    from {{ source('eco_transactional', 'CUSTOMER') }}
)

select * from raw
