import os 
import pandas as pd

import logging

def read_immigration_data():
    """
    uses pandas to read the immigration data into a df.\
    parameters:
        None
    returns:
        pandas df of the immigration data
    """

    data_directory = '/usr/local/data/immigration-data'

    data_files = [os.path.join(data_directory, f) for f in os.listdir(data_directory)]

    logging.info(f"ReadImmigration::Parquet Data Files::{data_files}")

    dfs = []

    for f in data_files:
        _df = pd.read_parquet(f)
        dfs.append(_df)
    
    immigration_data = pd.concat(dfs)

    logging.info(f"Read Parquet Files::{immigration_data.columns}")
    logging.info(f"Read all Parquet Files::{immigration_data.head()}")

    return immigration_data    
