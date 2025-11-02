{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

-- Deduplicate products and generate a surrogate key.

select
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_key,
    p.product_id,
    p.product_name,
    p.product_type,
    p.list_price as price
from (
    select
        product_id,
        product_name,
        product_type,
        list_price,
        fivetran_synced_ts,
        row_number() over (
            partition by product_id
            order by fivetran_synced_ts desc
        ) as rn
    from {{ ref('stg_product') }} 
) p
where p.rn = 1
