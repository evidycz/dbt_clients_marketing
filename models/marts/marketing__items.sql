with items as (

    select * from {{ ref('source_google_analytics__items') }}
)

select * from items