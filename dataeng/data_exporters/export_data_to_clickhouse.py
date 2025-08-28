if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import pandas as pd

@data_exporter
def export_data_to_clickhouse(df: pd.DataFrame, *args, **kwargs) -> None:
    """
    Export data to ClickHouse with the specified table structure.
    """
    from clickhouse_driver import Client
    
    # ClickHouse connection settings
    settings = {
        'host': 'clickhouse-server',
        'port': '9000',
        'user': 'etl_user',
        'password': 'your_strong_password_123',
        'database': 'etl'
    }
    
    # Initialize ClickHouse client
    client = Client(**settings)
    
    # Create table if not exists (matching DuckDB structure)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS mart_overall_performance (
        year                        BigInt,
        month_name                  String,
        total_sales                 Float64,
        total_profit                Float64,
        avg_profit_margin           Float64,
        avg_actual_shipment_days    Float64,
        avg_scheduled_shipment_days Float64
    ) ENGINE = MergeTree()
    ORDER BY (year, month_name)
    """
    
    client.execute(create_table_query)
    
    # Prepare data for insertion
    data = df.to_dict('records')
    
    # Insert data
    client.execute(
        "INSERT INTO mart_overall_performance VALUES",
        data,
        types_check=True
    )
    
    client.disconnect()
