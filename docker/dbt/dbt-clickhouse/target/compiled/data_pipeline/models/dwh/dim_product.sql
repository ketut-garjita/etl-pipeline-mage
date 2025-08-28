

select
    -- identifiers
    
    
    cast(Product_Card_Id as Int32)
 as product_card_id,
    
    
    cast(Product_Category_Id as Int32)
 as product_category_id,
    
    -- attributes
    
    
    cast(Category_Name as String)
 as category_name,
    
    
    cast(Product_Description as String)
 as product_description,
    
    
    cast(Product_Image as String)
 as product_image,
    
    
    cast(Product_Name as String)
 as product_name,
    
    
    cast(Product_Price as Float32)
 as product_price,
    
    
    cast(Product_Status as String)
 as product_status

from `etl`.`stg_product`