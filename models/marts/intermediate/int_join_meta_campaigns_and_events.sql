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

purchase_event as (

    select
        parent_row_id,
        sum(event_count) as event_count,
        sum(event_value) as event_value
    from events
    where event_name = 'purchase'
    group by 1
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
        coalesce(event_count, 0) as conversions,
        coalesce(event_value, 0) as conversion_value,
        cost

    from campaigns
    left join purchase_event
        on campaigns.row_id = purchase_event.parent_row_id
)

select * from joined