select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select date
from CAR_SALES_DBB.analytics.fct_btc_indicators
where date is null



      
    ) dbt_internal_test