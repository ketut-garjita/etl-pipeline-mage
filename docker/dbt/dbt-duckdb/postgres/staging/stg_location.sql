{{
  config(
    materialized='table'
  )
}}

SELECT 
    "Order Zipcode" AS Order_Zipcode,
    "Order City" AS Order_City,
    "Order State" AS Order_State,
    "Order Region" AS Order_Region,
    "Order Country" AS Order_Country,
    CAST("Latitude" AS DOUBLE) AS Latitude,
    CAST("Longitude" AS DOUBLE) AS Longitude,
    CAST("Ingestion_Time" AS TIMESTAMP) AS ingestion_time
FROM {{ source('main','raw_location') }}
--WHERE "Order Zipcode" IS NOT NULL

