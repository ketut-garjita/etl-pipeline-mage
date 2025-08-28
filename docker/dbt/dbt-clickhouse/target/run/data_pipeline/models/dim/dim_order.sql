
  
  create view "warehouse"."main"."dim_order__dbt_tmp" as (
    

select
    -- identifiers
    
    
    cast(Order_Id as integer)
 as order_id,
    
    
    cast(Order_Customer_Id as integer)
 as order_customer_id,
    
    
    cast(Order_Item_Id as integer)
 as order_item_id,
    
    
    cast(Product_Card_Id as integer)
 as product_card_id,
    
    
    cast(Department_Id as integer)
 as department_id,
    
    -- attributes
    
    
    cast(Order_Date as TEXT)
 as order_date,
    
    
    cast(Order_Item_Discount as float)
 as order_item_discount,
    
    
    cast(Order_Item_Discount_Rate as float)
 as order_item_discount_rate,
    
    
    cast(Order_Item_Product_Price as float)
 as order_item_product_price,
    
    
    cast(Order_Item_Profit_Ratio as float)
 as order_item_profit_ratio,
    
    
    cast(Order_Item_Quantity as integer)
 as order_item_quantity,
    
    
    cast(Sales_per_customer as float)
 as sales_per_customer,
    
    
    cast(Sales as float)
 as sales,
    
    
    cast(Order_Item_Total as float)
 as order_item_total,
    
    
    cast(Order_Profit_Per_Order as float)
 as order_profit_per_order,
    
    
    cast(Order_Status as TEXT)
 as order_status

from "warehouse"."main"."stg_order"
  );
