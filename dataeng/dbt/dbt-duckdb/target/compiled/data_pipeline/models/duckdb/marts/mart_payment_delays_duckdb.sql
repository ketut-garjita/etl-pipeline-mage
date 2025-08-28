
select
    delivery_status,
    AVG(days_for_shipping_real - days_for_shipping_scheduled) AS avg_payment_delay,
    COUNT(1) AS count_of_orders
FROM "warehouse"."main"."dim_shipping_duckdb"
GROUP BY delivery_status