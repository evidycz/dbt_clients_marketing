{{
    config(
        enabled = var('meta_ads_enabled', True)
    )
}}

with source as (

    select *
    from {{ source('meta_ads', 'campaigns') }}
),

final as (

    select
        date_start as date_day,
        
        {{ dbt_utils.generate_surrogate_key(["date_start", "upper(_config_join_key)", "_config_account_id", "campaign_id"]) }} as row_key,
        {{ dbt_utils.generate_surrogate_key(["date_start", "upper(_config_join_key)", "lower(_config_source_medium)"]) }} as join_key,

        _dlt_id as row_id,
        _config_account_id as account_id,
        campaign_id as campaign_id,

        account_currency as system_currency,

        upper(_config_join_key) as key_name,
        upper(_config_name) as system_name,
        lower(_config_source_medium) as source_medium,
        campaign_name as campaign_name,
        'UNKNOWN' as campaign_status,

        coalesce(impressions, 0) as impressions,
        coalesce(inline_link_clicks, 0) as clicks,
        round(cast(coalesce(spend, 0.0) as numeric), 2) as cost
        
    from source
)

select * from final
