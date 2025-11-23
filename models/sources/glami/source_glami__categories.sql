{{
    config(
        enabled = var('glami_enabled', True)
    )
}}

with source as (

    select *
    from {{ source('glami', 'categories') }}
),

final as (

    select
        {{ adapter.quote('date') }} as date_day,

        {{ dbt_utils.generate_surrogate_key(["date", "upper(_config_join_key)", "_config_account_id", "device", "brand", "category"]) }} as row_key,
        {{ dbt_utils.generate_surrogate_key(["date", "upper(_config_join_key)", "lower(_config_source_medium)"]) }} as join_key,

        _config_account_id as account_id,
        replace(replace(lower(_config_source_medium), ' ', ''), '/', '_')  as campaign_id,

        currency_code as system_currency,

        upper(_config_join_key) as key_name,
        upper(_config_name) as system_name,
        lower(_config_source_medium) as source_medium,
        'glami' as campaign_name,
        'unknown' as campaign_status,

        0 as impressions,
        coalesce(exit_clicks, 0) as clicks,
        coalesce(orders, 0) as conversions,
        coalesce(gmv, 0) as conversion_value,
        round(cast(coalesce(costs, 0) as numeric), 2) as cost

    from source
)

select * from final