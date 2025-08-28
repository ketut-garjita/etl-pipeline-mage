

select
    -- attributes
    
    
    cast(Shipping_date as String)
 as shipping_date,
    
    
    cast(Days_for_shipping_real as Int32)
 as days_for_shipping_real,
    
    
    cast(Days_for_shipment_scheduled as Int32)
 as days_for_shipping_scheduled,
    
    
    cast(Shipping_Mode as String)
 as shipping_mode,
    
    
    cast(Delivery_Status as String)
 as delivery_status

from `etl`.`stg_shipping`