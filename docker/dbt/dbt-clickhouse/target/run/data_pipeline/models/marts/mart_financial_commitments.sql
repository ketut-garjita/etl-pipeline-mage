
  
    
    
    
        
         


        insert into `etl`.`mart_financial_commitments__dbt_backup`
        ("department_name", "market", "category_name", "total_committed_funds", "commitment_fulfillment_rate")

WITH order_aggregates AS (
    SELECT 
        o.department_id,
        o.product_card_id,
        SUM(o.order_item_total) AS total_committed_funds,
        SUMIf(1, o.order_status = 'COMPLETED') AS completed_orders,
        COUNT() AS total_orders
    FROM `etl`.`fact_order` o
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
    INNER JOIN `etl`.`dim_department` d 
        ON CAST(oa.department_id AS String) = d.department_id
    INNER JOIN `etl`.`dim_product` p 
        ON oa.product_card_id = p.product_card_id
)
SELECT
    department_name,
    market,
    category_name,
    total_committed_funds,
    completed_orders / total_orders AS commitment_fulfillment_rate
FROM department_market_category
  