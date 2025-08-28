def run_duckdb_transformation():
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, lit, current_timestamp, trim, to_timestamp
    import duckdb
    import pandas as pd
    import os 
    from pyspark.sql.functions import coalesce

    # Set environment variables
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64/'
    os.environ['PATH'] = os.environ['JAVA_HOME'] + '/bin:' + os.environ['PATH']

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("duckdb_transformation") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print('Loading source data from DuckDB')
    con = duckdb.connect("/home/src/dataeng/dbt/warehouse.duckdb")
    df_pd = con.execute("SELECT * FROM supply_chain_data").fetchdf()
    df_raw = spark.createDataFrame(df_pd)

    # Column definitions
    customer_columns = ["Customer Id", "Customer Email", "Customer Fname", "Customer Lname",
                      "Customer Segment", "Customer City", "Customer Country",
                      "Customer State", "Customer Street", "Customer Zipcode"]
    
    product_columns = ["Product Card Id", "Product Category Id", "Category Name", "Product Description",
                       "Product Image", "Product Name", "Product Price", "Product Status", "Ingestion_Time"]

    location_columns = ["Order Zipcode", "Order City", "Order State", "Order Region", "Order Country",
                        "Latitude", "Longitude", "Ingestion_Time"]

    order_columns = ["Order Id", "order date (DateOrders)", "Order Customer Id", "Order Item Id", 
                    "Product Card Id", "Order Item Discount", "Order Item Discount Rate",
                    "Order Item Product Price", "Order Item Profit Ratio", "Order Item Quantity",
                    "Sales per customer", "Sales", "Order Item Total", "Order Profit Per Order",
                    "Order Status", "Department Id"]
    
    shipping_columns = ["shipping date (DateOrders)", "Days for shipping (real)", "Days for shipment (scheduled)",
                        "Shipping Mode", "Delivery Status", "Ingestion_Time"]

    department_columns = ["Department Id", "Department Name", "Market", "Ingestion_Time"]

    metadata_columns = ["key", "offset", "partition", "time", "topic", "Ingestion_Time"]


    # Helper functions
    def extract_columns(df, columns_to_extract):
        return df.select(*[col("data").getItem(c).alias(c) for c in columns_to_extract])
    
    def extract_metadata(df, columns_to_extract):
        return df.select(*[col("metadata").getItem(c).alias(c) for c in columns_to_extract])

    def create_raw_table(df, extract_func, columns_to_extract, add_timestamp=True):
        df_extracted = extract_func(df, columns_to_extract)
        if add_timestamp:
            df_extracted = df_extracted.withColumn("Ingestion_Time", current_timestamp())
        return df_extracted


    # Process order data with proper date handling
    print("Processing order data...")
    raw_order = create_raw_table(df_raw, extract_columns, order_columns)
    
    # Debug: Show sample data before conversion
    print("Sample data before date conversion:")
    raw_order.select("order date (DateOrders)").show(5, truncate=False)
    
    # Convert date with multiple possible formats
    raw_order = raw_order.withColumn(
        "order date (DateOrders)",
        coalesce(
            to_timestamp(trim(col("order date (DateOrders)")), "M/d/yyyy H:mm"),
            to_timestamp(trim(col("order date (DateOrders)")), "MM/dd/yyyy H:mm")
        )
    )

    # Show results after conversion
    print("Sample data after date conversion:")
    raw_order.select("order date (DateOrders)").show(5, truncate=False)
    
    
    # Process shipping data with proper date handling
    print("Processing shipping data...")
    raw_shipping = create_raw_table(df_raw, extract_columns, shipping_columns)
    
    # Debug: Show sample data before conversion
    print("Sample data before date conversion:")
    raw_shipping.select("shipping date (DateOrders)").show(5, truncate=False)
    
    # Convert date with multiple possible formats
    raw_shipping= raw_shipping.withColumn(
        "shipping date (DateOrders)",
        coalesce(
            to_timestamp(trim(col("shipping date (DateOrders)")), "M/d/yyyy H:mm"),
            to_timestamp(trim(col("shipping date (DateOrders)")), "MM/dd/yyyy H:mm")
        )
    )

    # Show results after conversion
    print("Sample data after date conversion:")
    raw_shipping.select("shipping date (DateOrders)").show(5, truncate=False)
    
    # Handle empty/missing dates
    raw_shipping = raw_shipping.withColumn(
        "shipping date (DateOrders)",
        when(col("shipping date (DateOrders)").isNull(), None)
        .otherwise(col("shipping date (DateOrders)"))
    )

    # Process other tables
    raw_customer = create_raw_table(df_raw, extract_columns, customer_columns)
    raw_product = create_raw_table(df_raw, extract_columns, product_columns)
    raw_location = create_raw_table(df_raw, extract_columns, location_columns)
    raw_department = create_raw_table(df_raw, extract_columns, department_columns)
    raw_metadata = create_raw_table(df_raw, extract_metadata, metadata_columns)
    
        # Create dictionary of all processed tables
    processed_tables = {
        "raw_order": raw_order,
        "raw_shipping": raw_shipping,
        "raw_customer": raw_customer,
        "raw_product": raw_product,
        "raw_location": raw_location,
        "raw_department": raw_department,
        "raw_metadata": raw_metadata
    }

    # Save to DuckDB
    for table_name, df in processed_tables.items():
        print(f"\nProcessing {table_name}...")
        
        # Convert to Pandas
        df_pd = df.toPandas()
        
        # Show schema info
        print(f"\nSchema for {table_name}:")
        print(df_pd.info())
        
        # Show sample data
        print(f"\nSample data for {table_name}:")
        print(df_pd.head(3))
        
        # Check if table exists
        table_exists = con.execute(f"""
            SELECT count(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0] > 0
        
        if not table_exists:
            # Create table if not exists
            print(f"Creating new table {table_name}...")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_pd")
        else:
            # Insert data if table exists
            print(f"Inserting into existing table {table_name}...")
            
            # Check schema compatibility first
            try:
                con.execute(f"INSERT INTO {table_name} SELECT * FROM df_pd")
                print(f"Inserted {len(df_pd)} records to {table_name}")
            except Exception as e:
                print(f"Error inserting into {table_name}: {str(e)}")
                print("Attempting to append with schema migration...")
                
                # Get existing columns
                existing_cols = [col[0] for col in con.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """).fetchall()]
                
                # Get new columns
                new_cols = df_pd.columns.tolist()
                
                # Find common columns
                common_cols = list(set(existing_cols) & set(new_cols))
                
                if not common_cols:
                    print("No common columns between source and target table")
                    continue
                
                # Insert only common columns
                con.execute(f"""
                    INSERT INTO {table_name} ({','.join(common_cols)})
                    SELECT {','.join(common_cols)} FROM df_pd
                """)
                print(f"Inserted {len(df_pd)} records using common columns only")

    # First convert all DataFrames to Pandas while Spark is still active
    processed_tables_pandas = {
        table_name: df.toPandas()
        for table_name, df in processed_tables.items()
    }

    # Cleanup
    spark.stop()
    con.close()
    print("\nTransformation completed successfully!")

    # Return the Pandas DataFrames
    return processed_tables_pandas


