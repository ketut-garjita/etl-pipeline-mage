

SELECT 
    CAST("Product Card Id" AS INTEGER) AS Product_Card_Id,
    CAST("Product Category Id" AS INTEGER) AS Product_Category_Id,
    "Category Name" AS Category_Name,
    CAST("Product Description" AS VARCHAR) AS Product_Description,
    "Product Image" AS Product_Image,
    "Product Name" AS Product_Name,
    "Product Price" AS Product_Price,
    CASE 
        WHEN LOWER("Product Status") = 'active' THEN 1
        WHEN LOWER("Product Status") = 'true' THEN 1
        WHEN "Product Status" = '1' THEN 1
        ELSE 0
    END::BIT AS Product_Status,
    CAST("Ingestion_Time" AS TIMESTAMP) AS ingestion_time
FROM `main`.`raw_product`
--WHERE "Product Card Id" IS NOT NULL