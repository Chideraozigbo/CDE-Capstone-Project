
    
    

select
    country_code as unique_field,
    count(*) as n_records

from country_database._mart_schema.dim_country
where country_code is not null
group by country_code
having count(*) > 1


