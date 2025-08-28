import duckdb
import pandas as pd
import os

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs) -> pd.DataFrame:
    # If the input is a list of dicts, convert it to a DataFrame.
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data  # assumed to be in the form of a DataFrame

    
    db_path = '/home/src/dataeng/dbt/warehouse.duckdb'
    table_name = 'supply_chain_data'

    #con = duckdb.connect(db_path, config={'allow_concurrent_connections': True})
    con = duckdb.connect(db_path)
  
    # Create a table if it doesn't exist yet
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT * FROM df LIMIT 0
    """)  # Note: you can also use safer CREATE logic

    # Insert data
    # con.execute("INSERT INTO supply_chain_data SELECT * FROM df")
    df.to_parquet("/tmp/batch.parquet")
    con.execute(f"COPY {table_name} FROM '/tmp/batch.parquet' (FORMAT PARQUET)")

    os.chmod(db_path, 0o666)
    
    con.close()
