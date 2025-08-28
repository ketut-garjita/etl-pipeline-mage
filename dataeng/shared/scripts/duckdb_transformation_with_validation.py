from schema_definitions import schema_definitions
from schema_validation import validate_schema
from logger import get_logger

from pyspark.sql.functions import col
from incremental_utils import get_max_time_from_duckdb
from logger import get_logger
from schema_validation import validate_schema
from schema_definitions import schema_definitions

logger = get_logger()

def run_duckdb_transformation():
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col   
    import duckdb
    import pandas as pd
    import os 

    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64/'
    os.environ['PATH'] = os.environ['JAVA_HOME'] + '/bin:' + os.environ['PATH']

    jar_file_path = "/home/src/dataeng-2/shared/config/gcs-connector-hadoop3-2.2.5.jar"

    customer_columns = ["Customer Id", "Customer Email", "Customer Fname", "Customer Lname",
                        "Customer Segment", "Customer City", "Customer Country",
                        "Customer State", "Customer Street", "Customer Zipcode", "ingestion_time"]
    product_columns = ["Product Card Id", "Product Category Id", "Category Name", "Product Description",
                       "Product Image", "Product Name", "Product Price", "Product Status", "ingestion_time"]
    location_columns = ["Order Zipcode", "Order City", "Order State", "Order Region", "Order Country",
                        "Latitude", "Longitude", "ingestion_time"]
    order_columns = ["Order Id", "Order date (DateOrders)", "Order Customer Id", "Order Item Id", "Product Card Id",
                     "Order Item Discount", "Order Item Discount Rate", "Order Item Product Price",
                     "Order Item Profit Ratio", "Order Item Quantity", "Sales per customer", "Sales",
                     "Order Item Total", "Order Profit Per Order", "Order Status", "Department Id", "ingestion_time"]
    shipping_columns = ["Shipping date (DateOrders)", "Days for shipping (real)", "Days for shipment (scheduled)",
                        "Shipping Mode", "Delivery Status", "ingestion_time"]
    department_columns = ["Department Id", "Department Name", "Market", "ingestion_time"]
    metadata_columns = ["key", "offset", "partition", "time", "topic"]

    # Create Spark Session
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Load from duckdb
    print(f'Loading data from DuckDB warehouse.duckdb')
    con = duckdb.connect("/home/src/dataeng-2/duckdb/data/warehouse.duckdb")
    df_pd = con.execute("SELECT * FROM supply_chain_stream").fetchdf()
    df_raw = spark.createDataFrame(df_pd)
    print("Loaded data from DuckDB")

    # Functions to extract columns
    def extract_columns(df, columns_to_extract):
        return df.select(*[col("data").getItem(c).alias(c) for c in columns_to_extract])

    def extract_metadata(df, columns_to_extract):
        return df.select(*[col("metadata").getItem(c).alias(c) for c in columns_to_extract])

    # Function to create dimension tables
    def create_dimension_tables(df, extract_func, columns_to_extract, table_name):
        return extract_func(df, columns_to_extract)

    # Create dimension tables
    stg_customer = create_dimension_tables(df_raw, extract_columns, customer_columns, "stg_customer")
    stg_product = create_dimension_tables(df_raw, extract_columns, product_columns, "stg_product")
    stg_location = create_dimension_tables(df_raw, extract_columns, location_columns, "stg_location")
    stg_order = create_dimension_tables(df_raw, extract_columns, order_columns, "stg_order") \
                        .withColumnRenamed('Order date (DateOrders)', 'Order date')
    stg_shipping = create_dimension_tables(df_raw, extract_columns, shipping_columns, "stg_shipping") \
                            .withColumnRenamed('Shipping date (DateOrders)', 'Shipping date') \
                            .withColumnRenamed('Days for shipping (real)', 'Days for shipping real') \
                            .withColumnRenamed('Days for shipment (scheduled)', 'Days for shipping scheduled')
    stg_department = create_dimension_tables(df_raw, extract_columns, department_columns, "stg_department")
    stg_metadata = create_dimension_tables(df_raw, extract_metadata, metadata_columns, "stg_metadata")

    # Dictionary of DataFrames
    dataframes = {
        "stg_customer": stg_customer,
        "stg_product": stg_product,
        "stg_location": stg_location,
        "stg_order": stg_order,
        "stg_shipping": stg_shipping,
        "stg_department": stg_department,
        "stg_metadata": stg_metadata
    }

    from pyspark.sql.functions import when, lit

    # Replace empty strings with null for all string columns
    # Di dalam loop penyimpanan:
    for name, dataframe in dataframes.items():
        #validate_schema(dataframe, schema_definitions[name], name)
        
        # handle "" as null
        for column, dtype in dataframe.dtypes:
            if dtype == 'string':
                dataframe = dataframe.withColumn(column, when(col(column) == "", None).otherwise(col(column)))

        df_pd = dataframe.toPandas()
        con.register(f"df_pd_{name}", df_pd)
        con.execute(f"INSERT INTO {name} SELECT * FROM df_pd_{name}")
        logger.info(f"Inserted {len(df_pd)} rows into {name}")
    

    # Di dalam loop insert
    for name, dataframe in dataframes.items():
        logger.info(f"Processing table: {name}")

        # Validasi skema
        # validate_schema(dataframe, schema_definitions[name], name)

        # Ambil max time dari tabel
        max_time = get_max_time_from_duckdb(con, name)
        logger.info(f"{name}: max_time = {max_time}")

        # Jika ada kolom `time`, lakukan filter incremental
        if "time" in dataframe.columns:
            dataframe = dataframe.filter(col("time") > max_time)
            count = dataframe.count()
            if count == 0:
                logger.info(f"No new records to insert into {name}. Skipping.")
                continue
            else:
                logger.info(f"{count} new records to insert into {name}.")

        # Handle empty strings jadi null
        for column, dtype in dataframe.dtypes:
            if dtype == 'string':
                dataframe = dataframe.withColumn(column, when(col(column) == "", None).otherwise(col(column)))

        # Insert
        df_pd = dataframe.toPandas()
        con.register(f"df_pd_{name}", df_pd)
        con.execute(f"INSERT INTO {name} SELECT * FROM df_pd_{name}")
        logger.info(f"Inserted {len(df_pd)} rows into {name}")
