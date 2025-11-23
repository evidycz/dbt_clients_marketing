{{
    config(
        enabled = var('meta_ads_enabled', True)
    )
}}

with campaigns as (

    select *
    from {{ ref('source_meta_ads__campaigns') }}
),

events as (

    select *
    from {{ ref('source_meta_ads__events') }}
),

joined as (

    select
        date_day,

        row_key,
        join_key,

        account_id,
        campaign_id,

        system_currency,

        key_name,
        system_name,
        source_medium,
        campaign_name,
        campaign_status,

        impressions,
        clicks,
        event_count as conversions,
        event_value as conversion_value,
        cost

    from campaigns
    left join events
        on campaigns.row_id = events.parent_row_id
)

select * from joined