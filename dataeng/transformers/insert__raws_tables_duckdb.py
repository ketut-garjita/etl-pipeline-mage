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
    import sys
    import os

    sys.path.append('/home/src/dataeng/shared/scripts')
    sys.path.append('/home/src/dataeng/shared/config')
    sys.path.append('/home/src/dataeng/shared/scripts/utils')

    from duckdb_raws import run_duckdb_transformation

   
    dataframes = run_duckdb_transformation()
    return dataframes

