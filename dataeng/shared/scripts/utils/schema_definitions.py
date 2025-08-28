# from pyspark.sql.types import *

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, FloatType, DoubleType, LongType
)

schema_definitions = {
    "customer_dimension": StructType([
        StructField("Customer_Id", IntegerType()),
        StructField("Customer_Email", StringType()),
        StructField("Customer_Fname", StringType()),
        StructField("Customer_Lname", StringType()),
        StructField("Customer_Segment", StringType()),
        StructField("Customer_City", StringType()),
        StructField("Customer_Country", StringType()),
        StructField("Customer_State", StringType()),
        StructField("Customer_Street", StringType()),
        StructField("Customer_Zipcode", IntegerType()),
        StructField("ingestion_time", LongType())
    ]),
    "product_dimension": StructType([
        StructField("Product_Card_Id", IntegerType()),
        StructField("Product_Category_Id", IntegerType()),
        StructField("Category_Name", StringType()),
        StructField("Product_Description", StringType()),
        StructField("Product_Image", StringType()),
        StructField("Product_Name", StringType()),
        StructField("Product_Price", StringType()),  # As VARCHAR
        StructField("Product_Status", BooleanType()),
        StructField("ingestion_time", LongType())
    ]),
    "location_dimension": StructType([
        StructField("Order_Zipcode", StringType()),
        StructField("Order_City", StringType()),
        StructField("Order_State", StringType()),
        StructField("Order_Region", StringType()),
        StructField("Order_Country", StringType()),
        StructField("Latitude", DoubleType()),
        StructField("Longitude", DoubleType()),
        StructField("ingestion_time", LongType())
    ]),
    "order_dimension": StructType([
        StructField("Order_Id", IntegerType()),
        StructField("Order_date_(DateOrders)", IntegerType()),
        StructField("Order_Customer_Id", IntegerType()),
        StructField("Order_Item_Id", IntegerType()),
        StructField("Product_Card_Id", IntegerType()),
        StructField("Order_Item_Discount", FloatType()),
        StructField("Order_Item_Discount_Rate", FloatType()),
        StructField("Order_Item_Product_Price", FloatType()),
        StructField("Order_Item_Profit_Ratio", StringType()),
        StructField("Order_Item_Quantity", IntegerType()),
        StructField("Sales_per_customer", FloatType()),
        StructField("Sales", FloatType()),
        StructField("Order_Item_Total", FloatType()),
        StructField("Order_Profit_Per_Order", FloatType()),
        StructField("Order_Status", StringType()),
        StructField("Department_Id", IntegerType()),
        StructField("ingestion_time", LongType())
    ]),
    "shipping_dimension": StructType([
        StructField("Shipping_date_(DateOrders)", IntegerType()),
        StructField("Days_for_shipping_(real)", IntegerType()),
        StructField("Days_for_shipment_(scheduled)", IntegerType()),
        StructField("Shipping_Mode", StringType()),
        StructField("Delivery_Status", StringType()),
        StructField("ingestion_time", LongType())
    ]),
    "department_dimension": StructType([
        StructField("Department_Id", IntegerType()),
        StructField("Department_Name", StringType()),
        StructField("Market", StringType()),
        StructField("ingestion_time", LongType())
    ]),
    "metadata_dimension": StructType([
        StructField("key", IntegerType()),
        StructField("offset", IntegerType()),
        StructField("partition", IntegerType()),
        StructField("time", LongType()),
        StructField("topic", StringType()),
    ]),
}
