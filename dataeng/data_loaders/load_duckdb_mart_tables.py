import duckdb
from pandas import DataFrame
from typing import Dict

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_duckdb(*args, **kwargs) -> Dict[str, DataFrame]:
    """
    Load multiple tables from DuckDB with verbose logging.
    Skip tables if they are empty.
    """
    duckdb_path = "/home/src/dataeng/dbt/warehouse.duckdb"  # update sesuai path

    try:
        conn = duckdb.connect(database=duckdb_path, read_only=False)

        tables = [
            'mart_customer_retention_rate',
            'mart_financial_commitments',
            'mart_fraud_detection',
            'mart_inventory_levels',
            'mart_payment_delays'
        ]

        result = {}
        for table in tables:
            try:
                df = conn.execute(f"SELECT * FROM {table}").fetchdf()
                row_count = len(df)

                if row_count == 0:
                    print(f"⚠️ Skipping {table}, empty DataFrame.")
                    continue

                print(f"📊 Loaded {row_count} rows from {table}")
                result[table] = df

            except Exception as e:
                print(f"❌ Error loading {table}: {e}")
                continue

        return result

    finally:
        if 'conn' in locals():
            conn.close()
