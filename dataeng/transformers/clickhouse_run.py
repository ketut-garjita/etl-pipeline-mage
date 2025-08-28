from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.bigquery import BigQuery
from os import path
from pandas import DataFrame
import pyspark 
import os 

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform_in_bigquery(*args, **kwargs) -> DataFrame:
    """
    Performs a transformation in Database
    """
    import sys
    import os

    # Tambahkan path untuk bisa import script eksternal
    sys.path.append('/home/src/dataeng-2/shared/scripts')
    sys.path.append('/home/src/dataeng-2/shared/config')

    from clickhouse_raws import run_clickhouse_transformation

    # Jalankan script OLAP dan ambil salah satu dataframe
    dataframes = run_clickhouse_transformation()  # ← pastikan fungsi ini mengembalikan dict DataFrame
    return dataframes

