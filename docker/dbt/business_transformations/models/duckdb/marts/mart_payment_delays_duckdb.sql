{{
    config(
        materialized='table',
        partition_by='delivery_status'
    )
}}
select
    delivery_status,
    AVG(days_for_shipping_real - days_for_shipping_scheduled) AS avg_payment_delay,
    COUNT(1) AS count_of_orders
FROM {{ ref('dim_shipping_duckdb') }}
GROUP BY delivery_status
