
  
    
    
    
        
         


        insert into `etl`.`mart_payment_delays__dbt_backup`
        ("delivery_status", "avg_payment_delay", "count_of_orders")
select
    delivery_status,
    AVG(days_for_shipping_real - days_for_shipment_scheduled) AS avg_payment_delay,
    COUNT(1) AS count_of_orders
FROM `etl`.`dim_shipping`
GROUP BY delivery_status
  