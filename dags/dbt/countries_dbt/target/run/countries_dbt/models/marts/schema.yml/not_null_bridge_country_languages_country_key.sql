select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select country_key
from country_database._mart_schema.bridge_country_languages
where country_key is null



      
    ) dbt_internal_test