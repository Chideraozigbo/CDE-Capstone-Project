
    
    

select
    Country_Name as unique_field,
    count(*) as n_records

from country_database.raw_country_schema.country_data
where Country_Name is not null
group by Country_Name
having count(*) > 1


