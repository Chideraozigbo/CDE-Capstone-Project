select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select language_id
from country_database._mart_schema.bridge_country_languages
where language_id is null



      
    ) dbt_internal_test