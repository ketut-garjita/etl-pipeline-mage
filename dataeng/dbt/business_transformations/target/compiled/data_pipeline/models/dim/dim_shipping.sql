

select
    -- attributes
    
    
    cast(Shipping_date as TEXT)
 as shipping_date,
    
    
    cast(Days_for_shipping_real as integer)
 as days_for_shipping_real,
    
    
    cast(Days_for_shipment_scheduled as integer)
 as days_for_shipping_scheduled,
    
    
    cast(Shipping_Mode as TEXT)
 as shipping_mode,
    
    
    cast(Delivery_Status as TEXT)
 as delivery_status

from "warehouse"."main"."stg_shipping"