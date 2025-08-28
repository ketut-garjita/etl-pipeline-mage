ATTACH TABLE _ UUID '8fb0a6d0-283c-4bff-894e-4799538b1ae3'
(
    `data` Tuple(Type String, Days_for_shipping_real String, Days_for_shipment_scheduled String, Benefit_per_order String, Sales_per_customer String, Delivery_Status String, Late_delivery_risk String, Category_Id String, Category_Name String, Customer_City String, Customer_Country String, Customer_Email String, Customer_Fname String, Customer_Id String, Customer_Lname String, Customer_Password String, Customer_Segment String, Customer_State String, Customer_Street String, Customer_Zipcode String, Department_Id String, Department_Name String, Latitude String, Longitude String, Market String, Order_City String, Order_Country String, Order_Customer_Id String, Order_Date_DateOrders String, Order_Id String, Order_Item_Cardprod_Id String, Order_Item_Discount String, Order_Item_Discount_Rate String, Order_Item_Id String, Order_Item_Product_Price String, Order_Item_Profit_Ratio String, Order_Item_Quantity String, Sales String, Order_Item_Total String, Order_Profit_Per_Order String, Order_Region String, Order_State String, Order_Status String, Order_Zipcode String, Product_Card_Id String, Product_Category_Id String, Product_Description String, Product_Image String, Product_Name String, Product_Price String, Product_Status String, Shipping_Date_DateOrders String, Shipping_Mode String),
    `metadata` Tuple(key String, partition String, offset Int32, time Int64, topic String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
