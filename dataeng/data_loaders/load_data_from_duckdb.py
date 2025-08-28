if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import duckdb
import pandas as pd
import os 

@data_loader
def load_data_from_duckdb(*args, **kwargs) -> pd.DataFrame:
    """
    Load data from DuckDB database.
    """
    import duckdb
    
    query = """
    SELECT 
        year,
        month_name,
        total_sales,
        total_profit,
        avg_profit_margin,
        avg_actual_shipment_days,
        avg_scheduled_shipment_days
    FROM mart_overall_performance
    """
    
    # Connect to DuckDB and execute query
    conn = duckdb.connect("/home/src/dataeng-2/dbt/warehouse.duckdb")
    df = conn.execute(query).fetchdf()
    conn.close()
    
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
