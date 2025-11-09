{{ config(
    materialized = 'table',
    schema = 'DW_GROUP6'          
) }}


select
    {{ dbt_utils.generate_surrogate_key(['merged.customer_id']) }} as customer_key,
    merged.customer_id,
    merged.first_name,
    merged.last_name,
    merged.phone,
    merged.email,
    merged.address,
    merged.city,
    merged.state,
    merged.zip_code,
    merged.country
from (

    select
        t.customer_id,
        coalesce(t.first_name, md.first_name)        as first_name,
        coalesce(t.last_name,  md.last_name)         as last_name,
        t.phone                                      as phone,
        coalesce(md.email, t.phone)                  as email,
        t.address,
        t.city,
        t.state,
        t.zip_code,
        t.country
    from (
        select
            c.customer_id,
            c.first_name,
            c.last_name,
            c.phone,
            c.address,
            c.city,
            c.state,
            c.zip        as zip_code,
            c.country
        from {{ ref('stg_customer') }} c
    ) t
    left join (
        select *
        from (
            select
                try_to_number(m.customer_id_str) as customer_id,
                m.first_name,
                m.last_name,
                null             as phone,
                m.subscriber_email as email,
                null             as address,
                null             as city,
                null             as state,
                null             as zip_code,
                null             as country,
                row_number() over (
                    partition by try_to_number(m.customer_id_str)
                    order by m.event_ts desc
                ) as rn
            from {{ ref('stg_marketing_emails') }} m
            where m.customer_id_str is not null
        ) dedup
        where dedup.rn = 1
    ) md
      on t.customer_id = md.customer_id

    union all

    select
        md.customer_id,
        md.first_name,
        md.last_name,
        md.phone,
        md.email,
        md.address,
        md.city,
        md.state,
        md.zip_code,
        md.country
    from (
        select *
        from (
            select
                try_to_number(m2.customer_id_str) as customer_id,
                m2.first_name,
                m2.last_name,
                null               as phone,
                m2.subscriber_email as email,
                null               as address,
                null               as city,
                null               as state,
                null               as zip_code,
                null               as country,
                row_number() over (
                    partition by try_to_number(m2.customer_id_str)
                    order by m2.event_ts desc
                ) as rn
            from {{ ref('stg_marketing_emails') }} m2
            where m2.customer_id_str is not null
        ) dedup2
        where dedup2.rn = 1
    ) md
    left join (
        select
            c.customer_id
        from {{ ref('stg_customer') }} c
    ) t
      on md.customer_id = t.customer_id
    where t.customer_id is null

) as merged

