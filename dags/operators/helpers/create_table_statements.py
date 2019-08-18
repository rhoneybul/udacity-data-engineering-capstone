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
CREATE TABLE IF NOT EXISTS immigrations (
    cicid  INTEGER NOT NULL,
    year INTEGER,
    month INTEGER,
    country_code INTEGER,
    port VARCHAR,
    arrival_date timestamp,
    mode INTEGER,
    address VARCHAR,
    departure_date timestamp,
    age INTEGER,
    visa INTEGER,
    count INTEGER,
    occupation VARCHAR,
    arrival_flag VARCHAR,
    departure_flag VARCHAR,
    update_flag VARCHAR,
    birth_year INTEGER,
    date_allowed_to VARCHAR,
    gender VARCHAR,
    ins_number VARCHAR,
    airline VARCHAR,
    admission_number INTEGER NOT NULL UNIQUE,
    flight_number VARCHAR,
    visa_type VARCHAR,
    PRIMARY KEY(cicid)
) 
DISTKEY (country_code) 
SORTKEY (arrival_date);
"""

## create global temperatures table
create_global_temperatures_table = """
CREATE TABLE IF NOT EXISTS global_temperatures (
    ts timestamp,
    average_temperature FLOAT,
    minimum_temperature FLOAT,
    maximum_temperature FLOAT
) 
DISTKEY (ts) 
SORTKEY (ts);
"""


## create global temperatures by country
create_global_temperatures_by_country = """
CREATE TABLE IF NOT EXISTS global_temperatures_by_country (
    ts timestamp,
    average_temperature FLOAT,
    country_code INTEGER
)
DISTKEY (ts)
SORTKEY (ts);
"""

create_countries = """
CREATE TABLE IF NOT EXISTS countries (
    country_code INTEGER NOT NULL,
    name VARCHAR,
    PRIMARY KEY (country_code)
)
DISTKEY (country_code);
"""

create_demographics = """
CREATE TABLE IF NOT EXISTS demographics (
    city_id INTEGER NOT NULL,
    median_age INTEGER,
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    number_of_veterans INTEGER, 
    foreign_born INTEGER,
    average_household_size FLOAT,
    race VARCHAR,
    count INTEGER,
    PRIMARY KEY (city_id)
)
diststyle all;
"""

create_cities = """
CREATE TABLE IF NOT EXISTS cities (
    city_id INTEGER NOT NULL IDENTITY(0,1),
    name VARCHAR,
    state VARCHAR,
    state_code VARCHAR,
    PRIMARY KEY (city_id)
) 
diststyle all;
"""

create_airport_codes = """
CREATE TABLE IF NOT EXISTS airport_codes (
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
    coordinates VARCHAR,
    PRIMARY KEY (id)
)
diststyle all;
"""

create_statements = {
    'immigrations': create_immigrations_table,
    'global_temperatures': create_global_temperatures_table,
    'global_temperatures_by_country': create_global_temperatures_by_country,
    'countries': create_countries,
    'demographics': create_demographics,
    'cities': create_cities,
    'airport_codes': create_airport_codes
}