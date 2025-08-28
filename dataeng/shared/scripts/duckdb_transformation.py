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
                        "Customer State", "Customer Street", "Customer Zipcode"]
    product_columns = ["Product Card Id", "Product Category Id", "Category Name", "Product Description",
                       "Product Image", "Product Name", "Product Price", "Product Status"]
    location_columns = ["Order Zipcode", "Order City", "Order State", "Order Region", "Order Country",
                        "Latitude", "Longitude"]
    order_columns = ["Order Id", "Order date (DateOrders)", "Order Customer Id", "Order Item Id", "Product Card Id",
                     "Order Item Discount", "Order Item Discount Rate", "Order Item Product Price",
                     "Order Item Profit Ratio", "Order Item Quantity", "Sales per customer", "Sales",
                     "Order Item Total", "Order Profit Per Order", "Order Status", "Department Id"]
    shipping_columns = ["Shipping date (DateOrders)", "Days for shipping (real)", "Days for shipment (scheduled)",
                        "Shipping Mode", "Delivery Status"]
    department_columns = ["Department Id", "Department Name", "Market"]
    metadata_columns = ["key", "offset", "partition", "time", "topic"]

    # Create Spark Session
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Load from duckdb
    print(f'Loading source data into dimension tables in DuckDB warehouse.duckdb')
    con = duckdb.connect("/home/src/dataeng-2/dbt/warehouse.duckdb")
    df_pd = con.execute("SELECT * FROM supply_chain_stream").fetchdf()
    df_raw = spark.createDataFrame(df_pd)

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

    cleaned_dataframes = {}

    for name, dataframe in dataframes.items():
        for column, dtype in dataframe.dtypes:
            if dtype == 'string':
                dataframe = dataframe.withColumn(column, when(col(column) == "", None).otherwise(col(column)))
        cleaned_dataframes[name] = dataframe

    for name, dataframe in cleaned_dataframes.items():
        df_pd = dataframe.toPandas()
        con.register(f"df_pd_{name}", df_pd)
        con.execute(f"INSERT INTO {name} SELECT * FROM df_pd_{name}")
        print(f"Inserted records into {name} in DuckDB\n")


    print("All dimension tables have been loaded in DuckDB !!!")

    spark.stop()



