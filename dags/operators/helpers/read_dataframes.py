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

def read_global_temperatures():
    """
    Reads the global temperatures into a pandas df.
    returns: 
        global_temperatures: pandas dataframe of global temperatures. 
    """

    data_path = "/usr/local/data/climate-change/GlobalTemperatures.csv"

    global_temperatures = pd.read_csv(data_path)

    logging.info(f'Reading Global Temperatures::{global_temperatures.head()}')

    return global_temperatures

def read_global_temperatures_by_country():
    """
    Reads the global temperatures by country into a pandas df.
    returns:
        global_temperatures_by_country: pandas dataframe of global
        temperatures by country.
    """

    data_path = "/usr/local/data/climate-change/GlobalTemperaturesByCountry.csv"

    global_temperatures_by_country = pd.read_csv(data_path)

    logging.info(f'Read Global Temperatures By Country::{global_temperatures_by_country.head()}')

def read_demographics():
    """
    Reads the demographics data into a pandas df
    returns:
        demographics: pandas dataframe of demographics
    """

    data_path = "/usr/local/demographics/us-cities-demographics.csv"

    demographics = pd.read_csv(data_path)

    logging.info(f'Read Demographics::{demographics.head()}')

    return demographics

def read_airport_codes():
    """
    Reads the airport code csv file
    returns:
        airport_codes: pandas dataframe of airport codes
    """

    data_path = "/usr/local/airport-codes_csv.csv"

    airport_codes = pd.read_csv(data_path)

    logging.info(f'Read Airport Codes::{airport_codes.head()}')

    return airport_codes