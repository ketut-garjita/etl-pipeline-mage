
  
    
    
    
        
         


        insert into `etl`.`mart_fraud_detection__dbt_backup`
        ("order_customer_id", "avg_order_total", "num_orders")

with fraud_detection AS (
    SELECT
        order_customer_id,
        AVG(order_item_total) AS avg_order_total,
        COUNT(*) AS num_orders
    FROM `etl`.`fact_order`
    GROUP BY order_customer_id
    ORDER BY avg_order_total DESC
)

select * from fraud_detection
  