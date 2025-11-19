with

sources as (

    select *
    from {{ ref('source_google_analytics__sources') }}
),

costs as (

    select
        date_day,
        join_key,
        system_name,
        source_medium,
        cost

    from {{ ref('int_union_campaigns') }}
),

source_categories as (

    select *
    from {{ ref('source_categories') }}
),

medium_categories as (

    select *
    from {{ ref('medium_categories') }}
),

aggregated_sources as (

    select
        date_day,
        join_key,

        key_name,
        source_medium,

        sum(sessions) as sessions,
        sum(engaged_sessions) as engaged_sessions,
        sum(total_users) as total_users,
        sum(new_users) as new_users,
        sum(purchases) as purchases,
        sum(revenue) as revenue,

    from sources
    group by 1, 2, 3, 4
),

aggregated_costs as (

    select
        date_day,
        join_key,
        system_name,
        source_medium,
        sum(cost) as cost,

    from costs
    group by 1,2,3,4
),

joined_sources_and_costs as (

    select
        aggregated_sources.*,

        coalesce(source_categories.source_category, 'UNKNOWN') as source_category,
        coalesce(medium_categories.medium_category, 'UNKNOWN') as medium_category,
        concat(coalesce(medium_categories.medium_category, 'UNKNOWN'), ' ', coalesce(source_categories.source_category, '')) as channel,

        aggregated_costs.system_name as system_name,
        aggregated_costs.cost as cost,

    from aggregated_sources
    left join aggregated_costs
        on aggregated_sources.join_key = aggregated_costs.join_key
    left join source_categories
        on trim(split(aggregated_sources.source_medium, '/')[safe_offset(0)]) = source_categories.source
    left join medium_categories
        on trim(split(aggregated_sources.source_medium, '/')[safe_offset(1)])= medium_categories.medium
)

select * from joined_sources_and_costs