{{
    config(
        materialized='table',
        partition_by={"field": "department_id", "data_type": "int64"},
        cluster_by=["department_id"]
    )
}}


WITH order_aggregates AS (
    SELECT
        o.department_id,
        o.product_card_id,
        SUM(o.order_item_total) AS total_committed_funds,
        SUM(CASE WHEN o.order_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders,
        COUNT(1) AS total_orders
    FROM (SELECT department_id, product_card_id, order_item_total, order_status FROM {{ ref('fact_order') }} LIMIT 50000) o
    GROUP BY o.department_id, o.product_card_id
),
department_market_category AS (
    SELECT
        d.department_name,
        d.market,
        p.category_name,
        oa.total_committed_funds,
        oa.completed_orders,
        oa.total_orders
    FROM order_aggregates oa
    JOIN (SELECT department_id, department_name, market FROM {{ ref('dim_department') }} LIMIT 50000) d
      ON oa.department_id = d.department_id
    JOIN (SELECT product_card_id, category_name FROM {{ ref('dim_product') }} LIMIT 50000) p
      ON oa.product_card_id = p.product_card_id
)
SELECT
    department_name,
    market,
    category_name,
    total_committed_funds,
    completed_orders / total_orders AS commitment_fulfillment_rate
FROM department_market_category

