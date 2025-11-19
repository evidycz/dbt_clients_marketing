{{
    config(
        enabled = var('seznam_sklik_enabled', True)
    )
}}


with source as (

    select *
    from {{ source('seznam_sklik', 'campaigns') }}
),

final as (

    select
        {{ adapter.quote('date') }} as date_day,

        {{ dbt_utils.generate_surrogate_key(["date", "upper(_config_join_key)", "_config_account_id", "id"]) }} as row_key,
        {{ dbt_utils.generate_surrogate_key(["date", "upper(_config_join_key)", "lower(_config_source_medium)"]) }} as join_key,

        _config_account_id as account_id,
        id as campaign_id,

        upper(_config_join_key) as key_name,
        upper(_config_name) as system_name,
        lower(_config_source_medium) as source_medium,
        name as campaign_name,
        upper(status) as campaign_status,

        coalesce(impressions, 0) as impressions,
        coalesce(clicks, 0) as clicks,
        coalesce(conversions, 0.0) as conversions,
        coalesce(conversion_value, 0.0) as conversion_value,
        round(cast(coalesce({{ dbt_utils.safe_divide('total_money', 100) }}, 0.0) as numeric), 2) as cost
        
    from source
)

select * from final
