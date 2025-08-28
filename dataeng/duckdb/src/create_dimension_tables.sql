CREATE TABLE stg_customer (
        Customer_Id  INTEGER,
        Customer_Email  VARCHAR,
        Custome_Fname  VARCHAR,
        Customer_Lname  VARCHAR,
        Customer_Segment  VARCHAR,
        Customer_City  VARCHAR,
        Customer_Country  VARCHAR,
        Customer_State  VARCHAR,
        Customer_Street  VARCHAR,
        Customer_Zipcode  INTEGER
);

CREATE TABLE stg_product (
    Product_Card_Id INTEGER,
    Product_Category_Id INTEGER,
    Category_Name VARCHAR,
    Product_Description VARCHAR,
    Product_Image VARCHAR,
    Product_Name VARCHAR,
    Product_Price VARCHAR,
    Product_Status BIT
);

CREATE TABLE stg_location (
    Order_Zipcode VARCHAR,
    Order_City VARCHAR,
    Order_State VARCHAR,
    Order_Region VARCHAR,
    Order_Country VARCHAR,
    Latitude DOUBLE,
    Longitude DOUBLE
);

CREATE TABLE stg_order (
    Order_Id INTEGER,
    Order_date_DateOrders INTEGER,
    Order_Customer_Id INTEGER,
    Order_Item_Id INTEGER,
    Product_Card_Id INTEGER,
    Order_Item_Discount FLOAT,
    Order_Item_Discount_Rate FLOAT,
    Order_Item_Product_Price FLOAT,
    Order_Item_Profit_Ratio VARCHAR,
    Order_Item_Quantity INTEGER,
    Sales_per_customer FLOAT,
    Sales FLOAT,
    Order_Item_Total FLOAT,
    Order_Profit_Per_Order FLOAT,
    Order_Status VARCHAR,
    Department_Id INTEGER
);

CREATE TABLE stg_shipping (
    Shipping_date_DateOrders INT32,
    Days_for_shipping_real INTEGER,
    Days_for_shipment_scheduled INTEGER,
    Shipping_Mode VARCHAR,
    Delivery_Status VARCHAR
);

CREATE TABLE stg_department (
    Department_Id INTEGER,
    Department_Name VARCHAR,
    Market VARCHAR
);

CREATE TABLE stg_metadata (
    key INT32,
    offset INTEGER,
    partition INTEGER,
    time BIGINT,
    topic VARCHAR
);

