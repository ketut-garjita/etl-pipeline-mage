{{ 
    config(
        materialized='table'
    ) 
}}

WITH month_names AS (
    SELECT 1 AS month_num, 'January' AS month_name UNION ALL
    SELECT 2, 'February' UNION ALL
    SELECT 3, 'March' UNION ALL
    SELECT 4, 'April' UNION ALL
    SELECT 5, 'May' UNION ALL
    SELECT 6, 'June' UNION ALL
    SELECT 7, 'July' UNION ALL
    SELECT 8, 'August' UNION ALL
    SELECT 9, 'September' UNION ALL
    SELECT 10, 'October' UNION ALL
    SELECT 11, 'November' UNION ALL
    SELECT 12, 'December'
),
total_inventory_value AS (
    SELECT
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        ROUND(SUM(order_item_product_price * order_item_quantity)::numeric, 2) AS total_inventory_value
    FROM
        {{ ref('fact_order') }}
    GROUP BY
        year, month
),
inventory_turnover AS (
    SELECT
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        ROUND((SUM(order_item_total)::numeric / NULLIF(AVG(order_item_quantity)::numeric, 0)), 2) AS inventory_turnover_ratio
    FROM 
        {{ ref('fact_order') }}
    GROUP BY 
        year, month
),
inventory_aging AS (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        CASE
            WHEN EXTRACT(DAY FROM AGE(order_date, DATE '2018-01-01')) <= 30 THEN '0-30 days'
            WHEN EXTRACT(DAY FROM AGE(order_date, DATE '2018-01-01')) <= 60 THEN '31-60 days'
            WHEN EXTRACT(DAY FROM AGE(order_date, DATE '2018-01-01')) <= 90 THEN '61-90 days'
            ELSE 'Over 90 days'
        END AS age_range,
        ROUND(SUM(order_item_product_price * order_item_quantity)::numeric, 2) AS inventory_value
    FROM 
        {{ ref('fact_order') }}
    GROUP BY 
        year, month, age_range
)

SELECT
    t.year,
    m.month_name,
    t.total_inventory_value,
    i.inventory_turnover_ratio,
    a.age_range,
    a.inventory_value
FROM 
    total_inventory_value t
JOIN 
    month_names m ON t.month = m.month_num
JOIN 
    inventory_turnover i ON t.year = i.year AND t.month = i.month
LEFT JOIN
    inventory_aging a ON t.year = a.year AND t.month = a.month
ORDER BY 
    t.year, t.month
