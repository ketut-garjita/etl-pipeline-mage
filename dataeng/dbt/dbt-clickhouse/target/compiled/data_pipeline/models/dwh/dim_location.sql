

select
    -- attributes
    
    
    cast(Order_Zipcode as String)
 as order_zipcode,
    
    
    cast(Order_City as String)
 as order_city,
    
    
    cast(Order_State as String)
 as order_state,
    
    
    cast(Order_Region as String)
 as order_region,
    
    
    cast(Order_Country as String)
 as order_country,
    
    
    cast(Latitude as Float32)
 as latitude,
    
    
    cast(Longitude as Float32)
 as longitude

from `etl`.`stg_location`