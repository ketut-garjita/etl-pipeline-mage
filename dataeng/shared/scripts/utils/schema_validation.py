from pyspark.sql.types import StructType

def validate_schema(df, expected_columns):
    """
    Validate if the number of columns in df matches the expected columns list.
    Data types are ignored.
    
    Parameters:
        df (DataFrame): Spark DataFrame to validate
        expected_columns (list): List of expected column names (just for count check)

    Returns:
        bool: True if valid, False otherwise
    """
    actual_column_count = len(df.columns)
    expected_column_count = len(expected_columns)

    if actual_column_count != expected_column_count:
        print(f"❌ Column count mismatch: expected {expected_column_count}, got {actual_column_count}")
        return False

    print("✅ Schema valid (column count matches)")
    return True

