{% set enabled_packages = get_enabled_packages() %}

with
{% for package in ['google_ads', 'meta_ads', 'seznam_sklik'] %}
{% if package in enabled_packages %}
{{ package }} as (

    select
        *
    from {{ ref('source_' ~ package ~ '__campaigns') }}
),
{% endif %}
{% endfor %}

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