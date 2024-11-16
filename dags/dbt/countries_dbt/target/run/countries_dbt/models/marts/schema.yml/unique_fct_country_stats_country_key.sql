select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    country_key as unique_field,
    count(*) as n_records

from country_database._mart_schema.fct_country_stats
where country_key is not null
group by country_key
having count(*) > 1



      
    ) dbt_internal_test