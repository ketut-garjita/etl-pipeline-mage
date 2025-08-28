
  
    
    

    create  table
      "warehouse"."main"."mart_payment_delays__dbt_tmp"
  
    as (
      
select
    delivery_status,
    AVG(days_for_shipping_real - days_for_shipping_scheduled) AS avg_payment_delay,
    COUNT(1) AS count_of_orders
FROM "warehouse"."main"."dim_shipping"
GROUP BY delivery_status
    );
  
  