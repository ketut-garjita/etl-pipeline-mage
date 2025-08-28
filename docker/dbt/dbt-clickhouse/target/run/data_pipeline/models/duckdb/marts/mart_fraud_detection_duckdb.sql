
  
    
    

    create  table
      "warehouse"."main"."mart_fraud_detection_duckdb__dbt_tmp"
  
    as (
      

with fraud_detection AS (
    SELECT
        order_customer_id,
        AVG(order_item_total) AS avg_order_total,
        COUNT(*) AS num_orders
    FROM "warehouse"."main"."fact_order_duckdb"
    GROUP BY order_customer_id
    ORDER BY avg_order_total DESC
)

select * from fraud_detection
    );
  
  