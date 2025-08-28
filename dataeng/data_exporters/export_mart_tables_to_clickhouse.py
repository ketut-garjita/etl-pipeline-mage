import os
from typing import Dict
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.clickhouse import ClickHouse

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_clickhouse(data: Dict[str, DataFrame], *args, **kwargs):
    """
    Export multiple pandas DataFrames to ClickHouse tables.
    Each key in `data` is the table name, value is the DataFrame.
    """

    config_path = os.path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    try:
        config = ConfigFileLoader(config_path, config_profile)
        exporter = ClickHouse(
            database=config['CLICKHOUSE']['database'],
            host=config['CLICKHOUSE']['host'],
            user=config['CLICKHOUSE']['user'],
            password=config['CLICKHOUSE']['password'],
            port=config['CLICKHOUSE']['port'],
        )

        for table_name, df in data.items():
            try:
                if df.empty:
                    print(f"⚠️ Skipping {table_name}, empty DataFrame.")
                    continue

                # Build schema mapping
                schema = []
                for col, dtype in zip(df.columns, df.dtypes):
                    ch_type = "String"
                    if "int" in str(dtype):
                        ch_type = "Int64"
                    elif "float" in str(dtype):
                        ch_type = "Float64"
                    schema.append(f"{col} Nullable({ch_type})")

                # Create table if not exists
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(schema)}
                ) ENGINE = MergeTree()
                ORDER BY tuple()
                """
                exporter.execute(create_sql)

                # Insert data
                exporter.export(
                    df,
                    table_name,
                    if_exists="append",  # use 'replace' if you want overwrite
                    chunk_size=10000,
                )

                print(f"✅ Exported {len(df)} rows to {table_name}")

            except Exception as e:
                print(f"❌ Error exporting {table_name}: {e}")
                continue

    except Exception as e:
        print(f"🔥 Connection/Config Error: {e}")
        raise

    finally:
        if "exporter" in locals():
            if hasattr(exporter, "close"):
                exporter.close()
