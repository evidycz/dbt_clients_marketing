with source as (

    select *
    from {{ source('google_analytics', 'events') }}
),

final as (

    select 
        {{ adapter.quote('date') }} as date_day,
        
        property_id as property_id,
        session_campaign_id as campaign_id,
        
        upper(config_group) as config_group,
        session_source_medium as source_medium,
        event_name,

        currency_code as analytics_currency,
        
        event_count,
        round(cast(event_value as numeric), 2) as event_value
    from source
)

select * from final