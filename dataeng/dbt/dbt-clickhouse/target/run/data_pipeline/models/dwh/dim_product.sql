
  
  create view "warehouse"."main"."dim_product__dbt_tmp" as (
    

select
    -- identifiers
    
    
    cast(Product_Card_Id as integer)
 as product_card_id,
    
    
    cast(Product_Category_Id as integer)
 as product_category_id,
    
    -- attributes
    
    
    cast(Category_Name as TEXT)
 as category_name,
    
    
    cast(Product_Description as TEXT)
 as product_description,
    
    
    cast(Product_Image as TEXT)
 as product_image,
    
    
    cast(Product_Name as TEXT)
 as product_name,
    
    
    cast(Product_Price as float)
 as product_price,
    
    
    cast(Product_Status as TEXT)
 as product_status

from "warehouse"."main"."stg_product"
  );
