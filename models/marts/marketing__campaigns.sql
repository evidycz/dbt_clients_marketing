with sources as (

    select *
    from {{ ref('source_google_analytics__sources') }}
),

sources_agg as (

   select
        campaign_id,
        date_day,

        sum(sessions) as sessions,
        sum(engaged_sessions) as engaged_sessions,
        sum(total_users) as total_users,
        sum(new_users) as new_users,
        sum(purchases) as purchases,
        sum(revenue) as revenue
    from sources
    group by 1, 2
),

campaigns as (

    select *
    from {{ ref('int_union_campaigns') }}
),

events as (

    select *
    from {{ ref('source_google_analytics__events') }}
),

events_agg as (
    select
        campaign_id,
        date_day,
        sum(case when event_name = 'view_item' then event_count else 0 end) as view_item,
        sum(case when event_name = 'add_to_cart' then event_count else 0 end) as add_to_cart,
        sum(case when event_name = 'begin_checkout' then event_count else 0 end) as begin_checkout,
        sum(case when event_name = 'purchase' then event_count else 0 end) as purchase
    from events
    group by 1, 2
),

joined as (

    select
        campaigns.*,
        
        coalesce(sources_agg.sessions, 0) as sessions,
        coalesce(sources_agg.engaged_sessions, 0) as engaged_sessions,
        coalesce(sources_agg.total_users, 0) as total_users,
        coalesce(sources_agg.new_users, 0) as new_users,
        coalesce(sources_agg.purchases, 0) as purchases,
        coalesce(sources_agg.revenue, 0) as revenue,

        coalesce(events_agg.view_item, 0 ) as viewed_item,
        coalesce(events_agg.add_to_cart, 0 ) as added_to_cart,
        coalesce(events_agg.begin_checkout, 0 ) as began_checkout,
        coalesce(events_agg.purchase, 0 ) as purchased_items

    from campaigns
    left join sources_agg
        on campaigns.campaign_id = sources_agg.campaign_id
        and campaigns.date_day = sources_agg.date_day
    left join events_agg
        on campaigns.campaign_id = events_agg.campaign_id
        and campaigns.date_day = events_agg.date_day
)

select * from joined
