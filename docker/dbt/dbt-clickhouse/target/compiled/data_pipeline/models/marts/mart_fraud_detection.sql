

with fraud_detection AS (
    SELECT
        order_customer_id,
        AVG(order_item_total) AS avg_order_total,
        COUNT(*) AS num_orders
    FROM `etl`.`fact_order`
    GROUP BY order_customer_id
)

select * from fraud_detection