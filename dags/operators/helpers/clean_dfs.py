import os 
import pandas as pd 
import datetime

import logging

def convert_sas_timestamp(column_name, df):
    df[column_name] = pd.to_timedelta(df[column_name], unit='D') + pd.Timestamp('1960-1-1')
    return df

def clean_immigration_data(df):
    """
    cleans the immigration data, and creates correct column names for the data frame.
    parameters:
        df: pandas data frame to be cleaned.
    returns:
        df: pandas data frame to be cleaned.
    """

    df = df.drop(['dtadfile', 'visapost', 'matflag'], axis=1)

    df.columns = ['cicid', 
                  'year',
                  'month',
                  'city',
                  'country', 
                  'port',
                  'arrival_date',
                  'mode',
                  'address',
                  'departure_date',
                  'age',
                  'visa',
                  'count',
                  'occupation',
                  'arrival_flag',
                  'departure_flag',
                  'update_flag',
                  'birth_year',
                  'date_allowed_to',
                  'gender',
                  'ins_number',
                  'airline',
                  'admission_number',
                  'flight_number',
                  'visa_type']

    logging.info('Cleaning datetime columns')

    df = convert_sas_timestamp('arrdate', df)
    df = convert_sas_timestamp('depdate', df)

    immigration_data = immigration_data[immigration_data['dtaddto'].str.len() == 8]

    immigration_data['dtaddto'] = pd.to_datetime(immigration_data['dtaddto'], format="%m%d%Y", errors='coerce')

    return df


def clean_global_temperatures(global_temperatures):
    """
    cleans the global tempeatures dataframe. converts the 'ts'
    column to datetime, and adds the correct column names.
    parameters:
        global_temperatures: pandas dataframe of global temperatures
    returns:
        global_temperatures: pandas dataframe, cleaned.
    """

    global_temperatures['ts'] = pd.to_datetime(global_temperatures['ts'], format="%Y-%m-%d")

    global_temperatures.columns = ['ts', 
                                   'average_temperature',
                                   'minimum_temperature',
                                   'maximum_temperature']

    return global_temperatures

def clean_global_temperatures_by_country(global_temperatures_by_country):
    """
    cleans the global temperatures by country dataframe. converts the ts 
    column to a datetime object and names the columns appropriately
    parameters:
        global_temperatures_by_country: pandas dataframe of global_temperatures
    returns:
        global_temperatures_by_country: pandas dataframe oof global temperatures
    """

    global_temperatures_by_country = pd.to_datetime(global_temperatures_by_country['ts'], format="%Y-%m-%d")

    global_temperatures_by_country.columns=['ts',
                                            'average_temperature',
                                            'country_code']

    return global_temperatures_by_country

def clean_demographics(demographics):
    """
    cleaned the demographics table. Added the relevant column names
    parameters:
        demographics: demographics pandas dataframe
    returns:
        demographics: demographics pandas dataframe
    """

    demographics.columns = ['city_id',
                            'median_age',
                            'male_population'
                            'female_population',
                            'total_population',
                            'number_of_veterans',
                            'foreign_born',
                            'average_household_size',
                            'race',
                            'count']

    return demographics

def clean_airport_codes(airport_codes):
    """
    cleaned the airport codes. added the relevant column names
    parameters:
        airport_codes: pandas dataframe for airport codes
    returns:
        airport_codes: pandas dataframe for airport codes
    """

    airport_codes.columns = ['id',
                             'type', 
                             'name',
                             'elevation_ft',
                             'continent',
                             'iso_country',
                             'iso_region',
                             'municipality',
                             'gps_code',
                             'iata_code',
                             'local_code',
                             'coordinates']

    return airport_codes