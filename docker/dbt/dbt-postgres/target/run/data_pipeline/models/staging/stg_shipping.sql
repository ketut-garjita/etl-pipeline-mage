
  
    

  create  table "etl"."public"."stg_shipping__dbt_tmp"
  
  
    as
  
  (
    

SELECT 
    "Shipping date (DateOrders)" AS Shipping_date,
    "Days for shipping (real)" AS Days_for_shipping_real,
    CAST("Days for shipment (scheduled)" AS INTEGER) AS Days_for_shipment_scheduled,
    "Shipping Mode" AS Shipping_Mode,
    "Delivery Status" AS Delivery_Status,
    CAST("Ingestion_Time" AS TIMESTAMP) AS ingestion_time
FROM "etl"."main"."raw_shipping"
--WHERE "Shipping date (DateOrders)" IS NOT NULL
  );
  