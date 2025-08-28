import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from config import credentials_path, jar_file_path, gcs_bucket_path, output_path
from utils import customer_columns, product_columns, location_columns, order_columns, shipping_columns, department_columns, metadata_columns

# Spark configuration
conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", jar_file_path) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path) \
    .set("spark.hadoop.google.cloud.project.id", "gothic-sylph-387906")

# Create Spark Context
sc = SparkContext(conf=conf)

# Hadoop configuration for GCS access
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_path)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Create Spark Session
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# Load data from GCS bucket
print(f'Loading Data from GCS Bucket path {gcs_bucket_path}')
df_raw = spark.read.parquet(gcs_bucket_path + 'raw_streaming/*')
print('Done')

# Function to extract specific columns from DataFrame
def extract_columns(df, columns_to_extract):
    extracted_cols = [col("data").getItem(col_name).alias(col_name) for col_name in columns_to_extract]
    return df.select(*extracted_cols)

# Function to create dimension tables
def create_dimension_tables(df, extract_func, columns_to_extract, table_name):
    dimension_df = extract_func(df, columns_to_extract)
    # Uncomment the line below if you want to create a temporary view for SQL queries
    # dimension_df.createOrReplaceTempView(table_name)
    return dimension_df

# Create dimension tables
stg_customer = create_dimension_tables(df_raw, extract_columns, customer_columns, "stg_customer")
# stg_customer.show()
print('Created Dimension table for Customers')

stg_product = create_dimension_tables(df_raw, extract_columns, product_columns, "stg_product")
# stg_product.show()
print('Created Dimension table for Products')

stg_location = create_dimension_tables(df_raw, extract_columns, location_columns, "stg_location")
# stg_location.show()
print('Created Dimension table for Location')

stg_order = create_dimension_tables(df_raw, extract_columns, order_columns, "stg_order").\
                    withColumnRenamed('Order date (DateOrders)','Order date')
# stg_order.show()
print('Created Dimension table for Orders')

stg_shipping = create_dimension_tables(df_raw, extract_columns, shipping_columns, "stg_shipping").\
                        withColumnRenamed('Shipping date (DateOrders)','Shipping date').\
                        withColumnRenamed('Days for shipping (real)','Days for shipping real').\
                        withColumnRenamed('Days for shipment (scheduled)','Days for shipping scheduled')                     
stg_shipping.show()
print('Created Dimension table for Shipping')

stg_department = create_dimension_tables(df_raw, extract_columns, department_columns, "stg_department")
# stg_department.show()
print('Created Dimension table for Departments')

# Function to extract metadata columns
def extract_metadata(df, columns_to_extract):
    extracted_cols = [col("metadata").getItem(col_name).alias(col_name) for col_name in columns_to_extract]
    return df.select(*extracted_cols)

# Create metadata dimension table
stg_metadata = create_dimension_tables(df_raw, extract_metadata, metadata_columns, "stg_metadata")
# stg_metadata.show()
print('Created Dimension table for Metadata')

# Dictionary to hold all dimension tables
dataframes = {
    "stg_customer": stg_customer,
    "stg_product": stg_product,
    "stg_location": stg_location,
    "stg_order": stg_order,
    "stg_shipping": stg_shipping,
    "stg_department": stg_department,
    "stg_metadata": stg_metadata
}

# Function to write dimension tables to GCS
def write_to_gcs(dataframes, output_path):
    print('Starting to Export Raw Streaming data to GCS...')
    for name, dataframe in dataframes.items():
        dataframe.write.mode("overwrite").option("header", "true").option("compression", "none").parquet(output_path + name + ".parquet")
        print(f"Exported Dataframe {name} to GCS.")

# Write dimension tables to GCS
write_to_gcs(dataframes, output_path)
