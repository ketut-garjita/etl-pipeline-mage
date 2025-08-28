if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def execute(**kwargs):
    import clickhouse_connect
    # Koneksi ke ClickHouse
    client = clickhouse_connect.get_client(
        host='clickhouse-server',  # ganti dengan IP ClickHouse Anda
        port=8123,               # default HTTP port ClickHouse
        username='etl_user',      # ganti jika pakai user lain
        password='your_strong_password_123',             # isi password jika ada
        database='etl'
    )

    # Query untuk membuat tabel
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test_table (
        id UInt32,
        name String,
        created_at DateTime DEFAULT now()
    ) 
    ENGINE = MergeTree
    ORDER BY id
    """

    # Eksekusi query
    client.command(create_table_query)
    print("Table 'test_table' created successfully in ClickHouse.")
