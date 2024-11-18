
    
    

with child as (
    select country_id as from_field
    from country_database._mart_schema.fact_country_metric
    where country_id is not null
),

parent as (
    select  as to_field
    from stg_country.country_id
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


