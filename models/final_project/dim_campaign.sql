{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['c.campaign_id']) }} as campaign_key,
    c.campaign_id,
    c.campaign_name,
    c.campaign_discount_pct as campaign_discount
from (
    select
        campaign_id,
        campaign_name,
        campaign_discount_pct,
        fivetran_synced_ts,
        row_number() over (
            partition by campaign_id
            order by fivetran_synced_ts desc
        ) as rn
    from {{ ref('stg_promotional_campaign') }} c
) c
where c.rn = 1
