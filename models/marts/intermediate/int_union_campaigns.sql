with

{% if var('google_ads_enabled', True) %}
google_ads_campaigns as (

    select *
    from {{ ref('source_google_ads__campaigns') }}
),
{% endif %}

{% if var('seznam_sklik_enabled', True) %}
seznam_sklik_campaigns as (

    select *
    from {{ ref('source_seznam_sklik__campaigns') }}
),
{% endif %}

{% if var('meta_ads_enabled', True) %}
meta_ads_campaigns as (

    select *
    from {{ ref('source_meta_ads__campaigns') }}
),


meta_ads_events as (

    select *
    from {{ ref('source_meta_ads__events') }}
),

join_meta_ads_campaigns_and_events as (

    select 
        date_day,
       
        row_key,
        join_key,

        account_id,
        campaign_id,
        
        key_name,
        system_name,
        source_medium,
        campaign_name,
        campaign_status,
        
        impressions,
        clicks,
        case when event_name = 'purchase' then event_count else 0 end as conversions,
        case when event_name = 'purchase' then event_value else 0 end as conversion_value,
        cost
        
    from meta_ads_campaigns
    left join meta_ads_events
        on meta_ads_campaigns.row_id = meta_ads_events.parent_row_id
),
{% endif %}



union_campaigns as (

    {% if var('google_ads_enabled', True) %}
    select *
    from google_ads_campaigns

    union all
    {% endif %}

    {% if var('seznam_sklik_enabled', True) %}
    select *
    from seznam_sklik_campaigns
    {% endif %}

    {% if var('meta_ads_enabled', True) %}
    union all

    select *
    from join_meta_ads_campaigns_and_events
    {% endif %}
)

select * from union_campaigns