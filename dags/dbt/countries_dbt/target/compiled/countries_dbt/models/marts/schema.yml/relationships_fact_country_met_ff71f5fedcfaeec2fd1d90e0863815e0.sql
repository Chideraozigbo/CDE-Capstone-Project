
    
    

with child as (
    select country_id as from_field
    from country_database._mart_schema.fact_country_metric
    where country_id is not null
),

parent as (
    select country_id as to_field
    from country_database._staging_schema.stg_country
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


