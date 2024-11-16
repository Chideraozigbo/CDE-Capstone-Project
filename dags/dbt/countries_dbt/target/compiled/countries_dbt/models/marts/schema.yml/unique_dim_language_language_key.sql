
    
    

select
    language_key as unique_field,
    count(*) as n_records

from country_database._mart_schema.dim_language
where language_key is not null
group by language_key
having count(*) > 1


