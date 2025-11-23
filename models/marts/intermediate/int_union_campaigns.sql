{% set enabled_packages = get_enabled_packages() %}

with
{% for package in ['google_ads', 'seznam_sklik'] %}
{% if package in enabled_packages %}
{{ package }} as (

    select
        *
    from {{ ref('source_' ~ package ~ '__campaigns') }}
),
{% endif %}
{% endfor %}

{% if 'meta_ads' in enabled_packages %}
meta_ads as (

    select
        *
    from {{ ref('int_join_meta_campaigns_and_events') }}
),
{% endif %}

{% if 'glami' in enabled_packages %}
glami as (

    select
        *
    from {{ ref('source_glami__categories') }}
),
{% endif %}

unioned as (
    {{ union_ctes(ctes=enabled_packages)}}
)

select * from unioned