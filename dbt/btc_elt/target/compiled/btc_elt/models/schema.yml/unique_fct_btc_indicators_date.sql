
    
    

select
    date as unique_field,
    count(*) as n_records

from CAR_SALES_DBB.analytics.fct_btc_indicators
where date is not null
group by date
having count(*) > 1


