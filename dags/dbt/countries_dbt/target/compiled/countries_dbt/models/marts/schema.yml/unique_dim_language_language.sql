
    
    

select
    language as unique_field,
    count(*) as n_records

from country_database._mart_schema.dim_language
where language is not null
group by language
having count(*) > 1


