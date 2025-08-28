import logging
import duckdb

logger = logging.getLogger("duckdb_pipeline")

# Map each table to its corresponding time column
TIME_COLUMNS = {
    'stg_customer': 'ingestion_time',
    'stg_product': 'ingestion_time',
    'stg_location': 'ingestion_time',
    'stg_order': 'ingestion_time',
    'stg_shipping': 'ingestion_time',
    'stg_department': 'ingestion_time',
    'stg_metadata': 'time'
}

def get_max_time_from_duckdb(table_name: str, time_column: str):
    try:
        result = duckdb.sql(f"SELECT MAX({time_column}) AS max_time FROM {table_name}")
        max_time = result.fetchone()[0]
        logger.info(f"Max time in {table_name}: {max_time}")
        return max_time
    except Exception as e:
        logger.warning(f"Failed to fetch max time from {table_name}: {e}")
        return None

def load_table_to_duckdb(df, table_name: str):
    logger.info(f"Processing table: {table_name}")

    # Tambahkan kolom ingestion_time jika perlu
    if 'ingestion_time' in df.columns and df['ingestion_time'].isnull().all():
        from datetime import datetime
        df['ingestion_time'] = datetime.now()

    time_column = TIME_COLUMNS.get(table_name)
    if not time_column:
        logger.warning(f"No time column defined for {table_name}. Skipping incremental check.")
        duckdb.sql("INSERT INTO {table_name} SELECT * FROM df")
        return

    max_time = get_max_time_from_duckdb(table_name, time_column)

    if max_time:
        try:
            filtered_df = df[df[time_column] > max_time]
        except Exception as e:
            logger.warning(f"Failed to filter DataFrame for {table_name}: {e}")
            filtered_df = df
    else:
        filtered_df = df

    inserted_rows = len(filtered_df)
    if inserted_rows > 0:
        duckdb.sql(f"INSERT INTO {table_name} SELECT * FROM filtered_df")
        logger.info(f"Inserted {inserted_rows} rows into {table_name}")
    else:
        logger.info(f"No new rows to insert into {table_name}")

