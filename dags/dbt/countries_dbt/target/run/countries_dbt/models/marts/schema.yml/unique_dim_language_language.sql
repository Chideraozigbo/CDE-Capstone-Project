select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    language as unique_field,
    count(*) as n_records

from country_database._mart_schema.dim_language
where language is not null
group by language
having count(*) > 1



      
    ) dbt_internal_test