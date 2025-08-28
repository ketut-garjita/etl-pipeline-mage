if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
    
from datetime import datetime
import pandas as pd
import clickhouse_connect

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs) -> pd.DataFrame:

    # Ensure DataFrame is flattened
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()
    
    # Convert the column names to a list using .tolist()
    
    # df = df[column_list]

    try:
        # Koneksi ke ClickHouse
        client = clickhouse_connect.get_client(
            host='clickhouse-server',
            port=8123,
            username='etl_user',
            password='your_strong_password_123',
            database='etl'
        )

        table_name = 'supply_chain_stream'

        create_table = """
       CREATE TABLE supply_chain_stream (
            data Tuple(
                Type String,
                `Days_for_shipping_real` String,
                `Days_for_shipment_scheduled` String,
                `Benefit_per_order` String,
                `Sales_per_customer` String,
                `Delivery_Status` String,
                `Late_delivery_risk` String,
                `Category_Id` String,
                `Category_Name` String,
                `Customer_City` String,
                `Customer_Country` String,
                `Customer_Email` String,
                `Customer_Fname` String,
                `Customer_Id` String,
                `Customer_Lname` String,
                `Customer_Password` String,
                `Customer_Segment` String,
                `Customer_State` String,
                `Customer_Street` String,
                `Customer_Zipcode` String,
                `Department_Id` String,
                `Department_Name` String,
                Latitude String,
                Longitude String,
                Market String,
                `Order_City` String,
                `Order_Country` String,
                `Order_Customer_Id` String,
                `Order_Date_DateOrders` String,
                `Order_Id` String,
                `Order_Item_Cardprod_Id` String,
                `Order_Item_Discount` String,
                `Order_Item_Discount_Rate` String,
                `Order_Item_Id` String,
                `Order_Item_Product_Price` String,
                `Order_Item_Profit_Ratio` String,
                `Order_Item_Quantity` String,
                Sales String,
                `Order_Item_Total` String,
                `Order_Profit_Per_Order` String,
                `Order_Region` String,
                `Order_State` String,
                `Order_Status` String,
                `Order_Zipcode` String,
                `Product_Card_Id` String,
                `Product_Category_Id` String,
                `Product_Description` String,
                `Product_Image` String,
                `Product_Name` String,
                `Product_Price` String,
                `Product_Status` String,
                `Shipping_Date_DateOrders` String,
                `Shipping_Mode` String
            ),
            metadata Tuple(
                key String,
                partition String,
                offset Int32,
                time Int64,
                topic String
            )
        )
        ENGINE = MergeTree()
        ORDER BY tuple();
        """
    
        client.command(create_table)

        print(f"Created {len(df)} rows into '{table_name}'")
    
        # Insert
        client.insert(
            table_name,
            df.values.tolist(),
            #column_names=column_list
        )

        #print(f"Inserted {len(df)} rows into supply_change_stream")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        client.close()

    # Return DataFrame untuk block berikutnya
    return df