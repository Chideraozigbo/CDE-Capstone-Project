
    
    

with child as (
    select currency_id as from_field
    from country_database._mart_schema.fact_country_metric
    where currency_id is not null
),

parent as (
    select  as to_field
    from dim_currency.currency_id
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


