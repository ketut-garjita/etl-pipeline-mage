from utils.schema_validation import validate_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

def test_valid_schema():
    data = [("a",), ("b",)]
    schema = StructType([StructField("col1", StringType(), True)])
    df = spark.createDataFrame(data, schema=schema)
    validate_schema(df, schema, "test_table")
