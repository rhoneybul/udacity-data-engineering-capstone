def drop_table_if_exists(table_name):
    """
    returns a sql statement to drop the immigration table if it exists.
    parameters:
        - table_name: <string> representing the table to be dropped.
    returns:
        - sql_statement: <string> drop table sql statement.
    """

    return f"""
    DROP TABLE IF EXISTS {table_name}
    """

## create immigrations table command.
create_immigrations_table = """
CREATE TABLE immigrations (
    year INTEGER,
    month INTEGER,
    country_code uuid,
    port VARCHAR,
    arrival_date timestamp,
    mode INTEGER,
    address VARCHAR,
    visa INTEGER,
    count INTEGER,
    occupation VARCHAR,
    arrival_flag VARCHAR,
    departure_flag VARCHAR,
    udpate_flag VARCHAR,
    date_of_birth timestamp,
    gender VARCHAR,
    ins_number VARCHAR,
    airline VARCHAR,
    admission_number INTEGER NOT NULL UNIQUE,
    flight_number VARCHAR,
    visa_type VARCHAR
) 
DISTKEY (country_code) 
SORTKEY (arrival_date) 
DISTSTYLE KEY;
"""

## create global temperatures table
create_global_temperatures_table = """
CREATE TABLE global_temperatures (
    ts timestamp,
    average_temperature FLOAT,
    minimum_temperature FLOAT,
    maximum_temperature FLOAT
) 
DISTKEY (ts) 
SORTKEY (ts)
DISTSTYLE KEY;
"""


## create global temperatures by country
create_global_temperatures_by_country = """
CREATE TABLE global_temperatures_by_country (
    ts timestamp,
    average_temperature FLOAT,
    country_code INTEGER
)
DISTKEY (ts)
SORTKEY (ts)
DISTSTYLE KEY;
"""

create_countries = """
CREATE TABLE countries (
    country_code INTEGER NOT NULL,
    name VARCHAR
)
DISTKEY (country_code)
PRIMARYKEY (country_code);
DISTSTYLE KEY;
"""

create_demographics = """
CREATE TABLE demographics (
    city_id VARCHAR,
    median_age INTEGER,
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    number_of_veterans INTEGER, 
    foreign_born INTEGER,
    average_household_size FLOAT,
    race VARCHAR,
    count INTEGER
)
PRIMARY KEY (city_id)
DISTSTYLE ALL;
"""

create_cities = """
CREATE TABLE cities (
    city_id uuid NOT NULL,
    name VARCHAR,
    state VARCHAR,
    state_code VARCHAR
) 
PRIMARY KEY (city_id)
DISTSTYLE ALL;
"""

create_airport_codes = """
CREATE TABLE airport_codes (
    id VARCHAR UNIQUE NOT NULL,
    type VARCHAR,
    name VARCHAR,
    elevation_ft FLOAT,
    continent VARCHAR,
    iso_country VARCHAR,
    iso_region VARCHAR,
    municipality VARCHAR,
    gps_code VARCHAR,
    iata_code VARCHAR,
    local_code VARCHAR,
    coordinates VARCHAR
)
PRIMARY KEY (id)
DISTSTYLE ALL;
"""