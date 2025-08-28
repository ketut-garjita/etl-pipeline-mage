from clickhouse_driver import Client
import pandas as pd
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs) -> pd.DataFrame:
    # Konversi input ke DataFrame jika perlu
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    df.head(5)

    # Konversi tipe data dan format tanggal
    #df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'], format='%m/%d/%Y %H:%M')
    #df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'], format='%m/%d/%Y %H:%M')
    
    # Untuk kolom numeric yang masih berupa string
    numeric_cols = [
        'Days for shipping (real)', 'Days for shipment (scheduled)',
        'Benefit per order', 'Sales per customer', 'Late_delivery_risk',
        'Category Id', 'Customer Id', 'Department Id', 'Order Customer Id',
        'Order Id', 'Order Item Cardprod Id', 'Order Item Discount',
        'Order Item Discount Rate', 'Order Item Id', 'Order Item Product Price',
        'Order Item Profit Ratio', 'Order Item Quantity', 'Sales',
        'Order Item Total', 'Order Profit Per Order', 'Product Card Id',
        'Product Category Id', 'Product Price', 'Product Status'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    import clickhouse_connect

    # Koneksi ke ClickHouse
    con = clickhouse_connect.get_client(
        host='clickhouse-server',  # ganti dengan IP ClickHouse Anda
        port=8123,               # default HTTP port ClickHouse
        username='etl_user',      # ganti jika pakai user lain
        password='your_strong_password_123',             # isi password jika ada
        database='etl'
    )

    table_name = 'supply_chain_stream'

    # Buat tabel dengan skema yang sesuai
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            `Type` String,
            `Days for shipping (real)` UInt8,
            `Days for shipment (scheduled)` UInt8,
            `Benefit per order` Float32,
            `Sales per customer` Float32,
            `Delivery Status` String,
            `Late_delivery_risk` UInt8,
            `Category Id` UInt32,
            `Category Name` String,
            `Customer City` String,
            `Customer Country` String,
            `Customer Email` String,
            `Customer Fname` String,
            `Customer Id` UInt32,
            `Customer Lname` String,
            `Customer Password` String,
            `Customer Segment` String,
            `Customer State` String,
            `Customer Street` String,
            `Customer Zipcode` String,
            `Department Id` UInt32,
            `Department Name` String,
            `Latitude` Float64,
            `Longitude` Float64,
            `Market` String,
            `Order City` String,
            `Order Country` String,
            `Order Customer Id` UInt32,
            `order date (DateOrders)` DateTime,
            `Order Id` UInt32,
            `Order Item Cardprod Id` UInt32,
            `Order Item Discount` Float32,
            `Order Item Discount Rate` Float32,
            `Order Item Id` UInt32,
            `Order Item Product Price` Float32,
            `Order Item Profit Ratio` Float32,
            `Order Item Quantity` UInt8,
            `Sales` Float32,
            `Order Item Total` Float32,
            `Order Profit Per Order` Float32,
            `Order Region` String,
            `Order State` String,
            `Order Status` String,
            `Order Zipcode` String,
            `Product Card Id` UInt32,
            `Product Category Id` UInt32,
            `Product Description` String,
            `Product Image` String,
            `Product Name` String,
            `Product Price` Float32,
            `Product Status` UInt8,
            `shipping date (DateOrders)` DateTime,
            `Shipping Mode` String,
            
            -- Kafka metadata fields
            `metadata__topic` String,
            `metadata__partition` UInt32,
            `metadata__offset` UInt64,
            `metadata__timestamp` DateTime
        ) ENGINE = MergeTree()
        ORDER BY (`order date (DateOrders)`, `Order Id`, `Order Item Id`)
    """)

    # Konversi data ke format yang sesuai untuk ClickHouse
    records = df.to_dict('records')
    
    # Insert data
    con.execute(
        f"INSERT INTO {table_name} VALUES",
        records,
        types_check=True
    )

    con.close()
    return df