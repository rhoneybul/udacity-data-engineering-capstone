# Data Engineering Capstone Project

## Project Summary

The project aims to take data relating to immigration, and perform ETL such that the data can be further analysed. The process will use airflow, and spark to co-ordinate the retrieval of the data, and transformation into fact and dimension tables. These will be stored in amazon redshift, such that a backend web service could then access, and subsequently serve insights into the dataset on request. 

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### 1. Scope the Project and Gather Data

I decided to use the data sets provided as part of the 'Udacity Provided Project'. This data consists of data related to immigration in the United States.

The data collected was as follows;

* *I94 Immigration Data* - This data was from the US National Tourism and Trade Office. The data contains international visitor arrival statistics by world regions, and select countries. The data contains the type of visa, the mode of transportation, the age groups, states visited, and the top ports of entry for immigration into the United States. The data was collected from [here](https://travel.trade.gov/research/reports/i94/historical/2016.html).
* *World Temperature Data* - This dataset contains global land temperatures, for world wide locations, over periods of time. The data was collected from [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
* *US Cities: Demographics* - This dataset contains information about the demographics of all US cities, and census-designated places with a population greater or equal to 65,000. The dataset can be accessed [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
* *Airport Code Table* - This dataset contains a simple table of airport codes, and corresponding cities. The data can be accessed [here](https://datahub.io/core/airport-codes#data).

Firstly we will aim to understand the schema of the data collected. The aim of the process will be to develop a data pipeline, such that the provided data can be transformed, cleaned, and loaded into a data warehouse. The aim will be to develop the data warehouse such that relevant insights can be extracted easily. We will have a few outcomes we wish to satisfy in the process;

* The data must be stored in fact, and dimensional tables.
* The data must be cleaned, such that it can be queryable. 
* The data must be stored such that database joins can be easily made to correlate data sources.

The aim will be to create the data warehouse, such that a backend web service could easily query the warehouse for information relating to international visitors. The main information and questions a user may want to extract form the data would be;

* Visitors by world regions.
* Visitors by demographics.
* Correlations between destination and source demographics.
* Correlations between destination and source climates.
* Correlations between immigration by source region, and the source regiopn temperature. 
* Correlations between visitor demographics, and states visited.

### 2. Explore and Assess the Data

We want to access the data such that we can determine the following;

* The schema of each relevant data source.
* The size of each relevant data source.
* The quality of each data source (how clean the data is).

This will enable us to determine the data model, the procedure required to clean the data, and the data quality checks we need to perform. 

The data exploration steps were performed using a jupyter notebook. The aim will be to read each data source into a dataframe using spark, and to subsequently assess the data. 

The jupyter notebook containing the relevant exploration is available in `./UdacityDendCapstone.ipynb`

The analysis completed in this jupyter notebook drove the decisions behind the schema chosen for the database tables.

### 3. Define the Data Model

The data model choice was driven by analysing the schemas in step 2. 

The following data model was chosen;

**immigrations**

The following table will serve as the fact table. This gives a record of all the immigrations into the united states.

| Table Name :: `immigrations`  
| - `ciid integer NOT NULL` _id for the immigration record, must be not null as to provide unique identifier_   
| - `year integer` _year of immigration_  
| - `month integer` _month of immigration_  
| - `country_code uuid` _source for immigration_  
| - `port varchar` _port addmitted through_  
| - `arrival_date date` _date of arrival_  
| - `mode integer` _mode of arrival_  
| - `address varchar` _state of arrival_  
| - `visa integer` _visa category_  
| - `count integer` _count, used for summary statistics_  
| - `occupation varchar` _occupation_  
| - `arrival_flag varchar` _whether admitted or paroled into the US_  
| - `departure_flag varchar` _whether departed, lost visa, or deceased_  
| - `update_flag varchar` _update of visa, either apprehended, overstayed, or updated to PR_  
| - `date_of_birth date` _date of birth_  
| - `gender varchar` _gender_  
| - `ins_number varchar` _INS number_  
| - `airline varchar` _airline which travelled on_  
| - `admission_number integer NOT NULL [UNIQUE]` _admission number, should be unique and not nullable_  
| - `flight_number varchar` _flight number travelled on_  
| - `visa_type varchar` _visa type_  

**global_temperatures**

The following table gives the global temperatures over time. To simplify the data model we will have only average land temperature, minimum land temperature, and max land temperature.

| Table Name :: `global_temperatures`   
| - `ts date` _date for the temperature record_    
| - `average_temperature float` _average temperature_  
| - `minimum_temperature float` _minimum temperature_  
| - `maximum_temperature float` _maximum temperature_  

**global_temperatures_by_country**

Since the source country for each immigration is given in country, we will just use the global temperature records based on country. A more granular data source would not provide any further value to the warehouse. We will use the average temperature only, as the uncertainty will not factor into the analysis performed on the warehouse. This will serve as a fact table.

| Table Name :: `global_temperatures_countries`  
| - `ts date` _date for the temperature record_  
| - `average_temperature float` _average temperature_  
| - `country_code integer` _country_  

**countries**

The following table will serve as a dimension table for all the countries. 

| Table Name :: `countries`  
| - `country_code integer` _country code_  
| - `name varchar` _country name_  

**demographics**

This table will provide the demographics for each city, by country code in the United States. This will form the fact table for city based demographics. We will have a dimension table for the city information to prevent duplicated data.

| Table Name :: `demographics`  
| - `city_id varchar` _city name_  
| - `median_age integer` _median age_  
| - `male_population integer` _males population in the city_  
| - `female_population integer` _female population in the city_  
| - `total_population integer` _total city population_  
| - `number_of_veterans integer` _total number of veterans in the city_  
| - `foreign_born integer` _total population of foreign born residents_  
| - `average_household_size float` _average number of residents per household_  
| - `race varchar` _race for the demographic statistic_  
| - `count integer` _number of residens satisfying the relevant demographic_  

**cities**

This table will provide a dimension table for the cities in the United States.

| Table Name :: `cities`
| - `city_id uuid UNIQUE` _uuid given for the city record_  
| - `name varchar` _city name_  
| - `state varchar` _state name_  
| - `state_code varchar` _state code_  

**airport_codes**

This table will serve as a dimension table, providing the codes for airports in the united states.

| Table Name :: `airport_codes`   
| - `id varchar UNIQUE NOT NULL` _identifier for the airport_
| - `type varchar` _the type of airport_
| - `name varchar` _the airport name_  
| - `elevation_ft float` _the elevation of the airport in feet_  
| - `continent varchar` _the continent of the airport_  
| - `iso_country varchar` _the country which the airport is in_  
| - `iso_region varchar` _the region which the aiport is in_  
| - `municipality varchar` _the municipality of the airport_  
| - `gps_code varchar` _the gps code for the airport_    
| - `iata_code varchar` _the iata code_  
| - `local_code varchar` _the local code used for the airport_  
| - `coordinates varchar` _the coordinates of the airport_  

#### Steps Required for Data Pipeline

The steps required to create the data pipeline, and load the data into the relevant tables is as follows;

1. Create the relevant tables, dropping them first if they exist.
2. Load relevant data to populate the cities, and countries dimension tables.
3. Load the data into the immigrations table, referencing the countries table when inserting.
4. Load the data into the temperature tables, referencing the countries table when inserting.
5. Load data into the demographic tables, referencing the cities table when inserting.
6. Load data into the airtport codes table.









