
    
    

with child as (
    select country_key as from_field
    from country_database._mart_schema.bridge_country_languages
    where country_key is not null
),

parent as (
    select country_key as to_field
    from country_database._mart_schema.dim_country
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


