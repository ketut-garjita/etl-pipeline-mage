

select
    -- identifiers
    
    
    cast(Order_Id as Int32)
 as order_id,
    
    
    cast(Order_Customer_Id as Int32)
 as order_customer_id,
    
    
    cast(Order_Item_Id as Int32)
 as order_item_id,
    
    
    cast(Product_Card_Id as Int32)
 as product_card_id,
    
    
    cast(Department_Id as Int32)
 as department_id,
    
    -- attributes
    
    
    cast(Order_Date as String)
 as order_date,
    
    
    cast(Order_Item_Discount as Float32)
 as order_item_discount,
    
    
    cast(Order_Item_Discount_Rate as Float32)
 as order_item_discount_rate,
    
    
    cast(Order_Item_Product_Price as Float32)
 as order_item_product_price,
    
    
    cast(Order_Item_Profit_Ratio as Float32)
 as order_item_profit_ratio,
    
    
    cast(Order_Item_Quantity as Int32)
 as order_item_quantity,
    
    
    cast(Sales_per_customer as Float32)
 as sales_per_customer,
    
    
    cast(Sales as Float32)
 as sales,
    
    
    cast(Order_Item_Total as Float32)
 as order_item_total,
    
    
    cast(Order_Profit_Per_Order as Float32)
 as order_profit_per_order,
    
    
    cast(Order_Status as String)
 as order_status

from `etl`.`stg_order`