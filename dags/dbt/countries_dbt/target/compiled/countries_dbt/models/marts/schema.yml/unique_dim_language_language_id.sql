
    
    

select
    language_id as unique_field,
    count(*) as n_records

from country_database._mart_schema.dim_language
where language_id is not null
group by language_id
having count(*) > 1


