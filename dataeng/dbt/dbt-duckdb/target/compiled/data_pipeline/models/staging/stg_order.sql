

SELECT 
    CAST("Order Id" AS INTEGER) AS Order_Id,
    --CAST("Order date (DateOrders)" AS TIMESTAMP) AS Order_Date,
    "Order date (DateOrders)" AS Order_Date,
    CAST("Order Customer Id" AS INTEGER) AS Order_Customer_Id,
    CAST("Order Item Id" AS INTEGER) AS Order_Item_Id,
    CAST("Product Card Id" AS INTEGER) AS Product_Card_Id,
    CAST("Order Item Discount" AS FLOAT) AS Order_Item_Discount,
    CAST("Order Item Discount Rate" AS FLOAT) AS Order_Item_Discount_Rate,
    CAST("Order Item Product Price" AS FLOAT) AS Order_Item_Product_Price,
    "Order Item Profit Ratio" AS Order_Item_Profit_Ratio,
    CAST("Order Item Quantity" AS INTEGER) AS Order_Item_Quantity,
    CAST("Sales per customer" AS FLOAT) AS Sales_per_customer,
    CAST("Sales" AS FLOAT) AS Sales,
    CAST("Order Item Total" AS FLOAT) AS Order_Item_Total,
    CAST("Order Profit Per Order" AS FLOAT) AS Order_Profit_Per_Order,
    "Order Status" AS Order_Status,
    CAST("Department Id" AS INTEGER) AS Department_Id,
    CAST("Ingestion_Time" AS TIMESTAMP) AS ingestion_time
FROM "warehouse"."main"."raw_order"
--WHERE "Order Id" IS NOT NULL