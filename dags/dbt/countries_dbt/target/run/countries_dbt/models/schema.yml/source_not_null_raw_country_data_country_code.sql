select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select country_code
from country_database.raw_country_schema.country_data
where country_code is null



      
    ) dbt_internal_test