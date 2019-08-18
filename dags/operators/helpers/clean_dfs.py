import os 
import pandas as pd 

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