
  
    

  create  table "etl"."gold"."mart_overall_performance__dbt_tmp"
  
  
    as
  
  (
    

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
overall_sales_performance AS (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        ROUND(SUM(order_item_total)::numeric, 2) AS total_sales,
        ROUND(SUM(order_profit_per_order)::numeric, 2) AS total_profit
    FROM "etl"."gold"."fact_order"
    GROUP BY year, month
),
profit_margin_analysis AS (
    SELECT 
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        ROUND(AVG(order_profit_per_order)::numeric, 2) AS avg_profit_margin
    FROM "etl"."gold"."fact_order"
    GROUP BY year, month
),
operational_efficiency_metrics AS (
    SELECT 
        ROUND(AVG(days_for_shipping_real)::numeric, 2) AS avg_actual_shipment_days,
        ROUND(AVG(days_for_shipment_scheduled)::numeric, 2) AS avg_scheduled_shipment_days
    FROM "etl"."gold"."dim_shipping"
)
SELECT 
    os.year,
    m.month_name,
    os.total_sales,
    os.total_profit,
    pma.avg_profit_margin,
    oem.avg_actual_shipment_days,
    oem.avg_scheduled_shipment_days
FROM 
    overall_sales_performance os
JOIN 
    profit_margin_analysis pma ON os.year = pma.year AND os.month = pma.month
JOIN
    month_names m ON os.month = m.month_num
CROSS JOIN 
    operational_efficiency_metrics oem
ORDER BY 
    os.year, os.month
  );
  