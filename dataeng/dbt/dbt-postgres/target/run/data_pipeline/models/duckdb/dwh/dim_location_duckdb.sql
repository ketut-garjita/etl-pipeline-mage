
  
  create view "warehouse"."main"."dim_location_duckdb__dbt_tmp" as (
    

select
    -- attributes
    
    
    cast(Order_Zipcode as TEXT)
 as order_zipcode,
    
    
    cast(Order_City as TEXT)
 as order_city,
    
    
    cast(Order_State as TEXT)
 as order_state,
    
    
    cast(Order_Region as TEXT)
 as order_region,
    
    
    cast(Order_Country as TEXT)
 as order_country,
    
    
    cast(Latitude as float)
 as latitude,
    
    
    cast(Longitude as float)
 as longitude

from "warehouse"."main"."stg_location_duckdb"
  );
