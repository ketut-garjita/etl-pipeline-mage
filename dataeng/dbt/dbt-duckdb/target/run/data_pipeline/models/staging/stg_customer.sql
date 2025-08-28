
  
    
    

    create  table
      "warehouse"."main"."stg_customer__dbt_tmp"
  
    as (
      

SELECT 
    "Customer Id" AS Customer_Id,
    "Customer Email" AS Customer_Email,
    "Customer Fname" AS Customer_Fname,
    "Customer Lname" AS Customer_Lname,
    "Customer Segment" AS Customer_Segment,
    "Customer City" AS Customer_City,
    "Customer Country" AS Customer_Country,
    "Customer State" AS Customer_State,
    "Customer Street" AS Customer_Street,
    "Customer Zipcode" AS Customer_Zipcode,
    CAST("Ingestion_Time" AS TIMESTAMP) AS ingestion_time
FROM "warehouse"."main"."raw_customer"
--WHERE "Customer Id" IS NOT NULL
    );
  
  