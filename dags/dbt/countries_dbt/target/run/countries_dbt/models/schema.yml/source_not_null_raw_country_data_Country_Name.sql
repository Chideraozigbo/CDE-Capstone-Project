select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select Country_Name
from country_database.raw_country_schema.country_data
where Country_Name is null



      
    ) dbt_internal_test