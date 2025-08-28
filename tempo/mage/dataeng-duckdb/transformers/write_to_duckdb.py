import duckdb
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(data, *args, **kwargs) -> pd.DataFrame:
    # Kalau input berupa list of dict, ubah ke DataFrame
    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = data  # diasumsikan sudah berupa DataFrame

    
    db_path = '/home/src/dataeng-duckdb/duckdb/data/warehouse.duckdb'
    table_name = 'supply_chain_stream'

    con = duckdb.connect(db_path)

    # Buat tabel jika belum ada
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT * FROM df LIMIT 0
    """)  # Note: bisa juga pakai safer CREATE logic

    # Insert data
    con.execute("INSERT INTO supply_chain_stream SELECT * FROM df")

    con.close()

    return df
