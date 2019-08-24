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

The following table will serve as the fact table. This gives a record of all the immigrations into the united states. We are going to choose a key distribution, and the distribution key to be the country code. This is because we will have to create joins between immigrations and country information.

| Table Name :: `immigrations`  
| - `cicid integer NOT NULL PRIMARY KEY` _id for the immigration record, must be not null as to provide unique identifier_   
| - `year integer` _year of immigration_  
| - `month integer` _month of immigration_  
| - `city integer` _source city for immigration_  
| - `country integer` _source country for immigration_ 
| - `port varchar` _port addmitted through_ 
| - `arrival_date timestamp SORT KEY` _date of arrival_  
| - `mode integer` _mode of arrival_  
| - `address varchar` _state of arrival_ 
| - `departure_date timestamp` _date of departure_
| - `age integer` _age in years_
| - `visa integer` _visa category_ 
| - `count integer` _count, used for summary statistics_  
| - `occupation varchar` _occupation_  
| - `arrival_flag varchar` _whether admitted or paroled into the US_  
| - `departure_flag varchar` _whether departed, lost visa, or deceased_  
| - `update_flag varchar` _update of visa, either apprehended, overstayed, or updated to PR_  
| - `birth_year integer` _four digit year of birth_  
| - `date_allowed_to VARCHAR` _character date field to when admitted in the US_
| - `gender varchar` _gender_  
| - `ins_number varchar` _INS number_  
| - `airline varchar` _airline which travelled on_  
| - `admission_number bigint NOT NULL [UNIQUE]` _admission number, should be unique and not nullable_ 
| - `flight_number varchar` _flight number travelled on_  
| - `visa_type varchar` _visa type_  

**global_temperatures**

The following table gives the global temperatures over time. To simplify the data model we will have only average land temperature, minimum land temperature, and max land temperature. We have chosen a key distribution, since this table will grow in size. The sort key is given by the date since we will want to query based on this column most frequently. 

| Table Name :: `global_temperatures`   
| - `ts timestamp DIST KEY SORT KEY` _date for the temperature record_    
| - `average_temperature float` _average temperature_  
| - `minimum_temperature float` _minimum temperature_  
| - `maximum_temperature float` _maximum temperature_  

**global_temperatures_by_country**

Since the source country for each immigration is given in country, we will just use the global temperature records based on country. A more granular data source would not provide any further value to the warehouse. We will use the average temperature only, as the uncertainty will not factor into the analysis performed on the warehouse. This will serve as a fact table. We have chosen a key distribution, We have chosen the ts to be the dist key and the sort key, since this will distributed the data evenly and the data will often be queried on this.

| Table Name :: `global_temperatures_countries`  
| - `ts timestamp SORT KEY DIST KEY` _date for the temperature record_  
| - `average_temperature float` _average temperature_  
| - `country varchar` _country_  

**countries**

The following table will serve as a dimension table for all the countries. We have chosen an 'all' distribution for the table, since it will not grow too large, by redshifts standards.

| Table Name :: `countries`  
| - `country_id integer NOT NULL PRIMARY KEY IDENTITY(0.1)` _country code_  
| - `name varchar` _country name_  

**demographics**

This table will provide the demographics for each city, by country code in the United States. This will form the fact table for city based demographics. We will have a dimension table for the city information to prevent duplicated data. We have chosen an all distribution for this table, since it will have slowly changing dimensions, and not grow too large. 

| Table Name :: `demographics`  
| - `city_id varchar NOT NULL PRIMARY KEY` _city name_  
| - `median_age integer` _median age_  
| - `male_population integer` _males population in the city_  
| - `female_population integer` _female population in the city_  
| - `total_population integer` _total city population_  
| - `number_of_veterans integer` _total number of veterans in the city_  
| - `foreign_born integer` _total population of foreign born residents_  
| - `average_household_size float` _average number of residents per household_  
| - `race varchar` _race for the demographic statistic_  
| - `count integer` _number of residens satisfying the relevant demographic_  

| - `state varchar` _state of the record_

| - `state_code varchar` _state code for the record_

**cities**

This table will provide a dimension table for the cities in the United States. We have chosen an 'all' distribution for this table, since it will not grow too large. 

| Table Name :: `cities`
| - `city_id integer NOT NULL PRIMARY KEY IDENTITY(0,1)` _city id which is given by the identity function_
| - `name varchar` _city name_  
| - `state varchar` _state name_  
| - `state_code varchar` _state code_  

**airport_codes**

This table will serve as a dimension table, providing the codes for airports in the united states. We have chosen an 'all' distribution for this table, since it will not grow too large. 

| Table Name :: `airport_codes`   
| - `id varchar UNIQUE NOT NULL PRIMARY KEY` _identifier for the airport_  
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

## 4. ETL 

### Steps Required For Data Pipeline

The steps required to create the data pipeline, and load the data into the relevant tables is as follows;

1. Create the relevant tables, dropping them first if they exist.
2. Load relevant data to populate the cities, and countries dimension tables.
3. Load the data into the immigrations table, referencing the countries table when inserting.
4. Load the data into the temperature tables, referencing the countries table when inserting.
5. Load data into the demographic tables, referencing the cities table when inserting.
6. Load data into the airtport codes table.

#### Redshift Setup

To test the pipeline, I created a redshift cluster on AWS, in the `ap-southeast-2` region.

I used a single node cluster.

#### Airflow Setup

I chose to use airflow for the project. The reason I chose to use airflow, was to be able to create the data pipeline easily. On top of this, airflow offers strong observability over the pipeline. To get started with airflow, I used the docker compose file provided in the root of the project.

To start airflow in the background, you just need to run;

```
$ docker-compose up -d
```

#### Configure Redshift Connection

I then configured airflow to connection to redshift, by creating a `postgres` connection in the airflow dashboard.

#### Data Input

The data inputs are csv files, and parquet files. To store the data for use with airflow, I added the `data` directory to the `/usr/local/data` directory in the airflow dockerfile, such that the operators could access it. 

#### Retrieving the Data 

To retrieve the data, you will need to pull the data directory using the aws cli. 

The data is stored in a public S3 bucket.

The command to retrieve the relevant data is;

```
$ aws s3 cp --recursive s3://honeybulr-udacity-capstone/data data
```

### ETL Steps

The following outlines the steps, and corresponding operators associated with the data pipeline;

#### 1. Create Tables 

This step is governed by the 'CreateTableOperator'. The operator is given a table name, and a redshift connection id. The create statements are exported by the 'create_table_statements' module in helpers. Given the table name, the sql statement is then returned, and the postgres hook executes the command.

#### 2. ETL

The ETL step then is responsible for extracting, transforming, and loading the data from disk to redshift. The data is stored in parquet, and .csv files. The ETL operators take three steps;

1. Read dataframe. This is the function which returns the dataframe from the relevant source.
2. Clean dataframe. This cleans the data frame appropriately. This will remove the unneccessary columns, add the correct column names, change timestamp columns, and remove unneccessary duplications.
3. Insert dataframe. This step inserts the dataframe into the redshift database. This uses the pandas `df.to_sql` function.

This step inserts data for the following tables;

* immigrations
* global_temperatures
* global_temperatures_by_country
* demographics
* airport_codes

#### 3. Dimension Table Creation

After the ETL processes have completed, we need to subsequently create the dimension tables. These use the 'CreateDimensionTable' operator. This will take a sql statement, that will create the relevant dimension tables, from the data inserted from the ETL process. This step creates the following tables;

* cities
* countries

#### 4. Data Quality Checks

The following ensures that the data has been inserted correctly. 

We will use a data quality check for every table to ensure data was inserted.

Hence, we will use the 'EnsureRecords' Operator, which ensures that for a given 'table_name', there exists records. 

Subsequently, we will ensure for the immigrations, airport_codes, cities and countries tables, that we have data uniqueness. For a given 'table_name', and 'column', we can verify that when grouped by that particular column, there exists the same number of records. This will prove that we are inserting unique records into the database. 



## 5. Project Write Up

### Project goal

The goal of the project was to create a data warehouse, with data relevant to immigration to the United States. This would include records of immigration, global temperatures over time, and demographics for the cities. We would want to run queries against the database, to extract data relating to the temperature factors which influenced particular migrations to the United States. We would want to query the warehouse to determine the particular regions which was popular for immigration, and which demographic factors were determined to strongly influence immigration to that location.

The model chosen was to create fact records for the immigration to the United States, temperatures, and demographics. As well as this, the model included dimension tables for cities and countries. This is because, this is the main dimensions we would wish to query against. We want to determine correlations between countries, and particular temperatures, as well as cities, and relevant demographics. 

We chose to use key distributions for immigrations, global_temperatures, and global_temperatures_by_country. This is due to the fact that these tables may be large, and we will want to query heavily against these. The other tables were given an all distribution, since they would not grow to the same extent, a key distribution was not neccessary.

With regards to spark and airflow, airflow was chosen to orchestrate the data pipeline. This tool was used, since it enables us to create strong observability over the data pipeline, and to ensure dependency on all tasks. Spark was not used, as the overhead for creating a spark cluster was not deemed necessary.

### Tools & Technologies

* Redshift - I used redshift, since this is a scalable data warehouse system. It is very easy to create, and manage, with strong observability. We are able to easily connect to this system with python, and if the quantity of data is large, we are able to scale the database horizontally, as well as vertically to deal with greater loading.
* Airflow - I used airflow, since this is a strong tool for creating pipelines with dependencies, and strong observability over the system. I was able to create a DAG to visualise the process which the pipeline would go through, and track errors, and successes of the pipeline. 

### Process Steps

The steps which I went through to complete the project was as follows;

* Download all the relevant data, from the sources provided.

* Use a jupyter notebook, to investigate the data, for the columns present in each data, the length, and how to convert the relevant timestamps. Data explorations can be observed in `./UdacityDendCapstone.ipynb`

* Set up airflow, using docker compose. The docker compose file is in `./docker-compose.yaml` To start this, you just need to run `$ docker-compose up -d`. This should start the relevant postgres database, and the webserver on `localhost:8000`. 

* Created a RedShift Cluster, in AWS, in the sydney region.

* Created a `Variable` for the Redshift connection, and a `Connection` for the connection to Redshift.

* Added the `Create Tables` operators, create table sql statements.

* Added the ETL operators.

* Added the data quality operators. 

* Created the DAG in airflow.

* Ran the DAG, and ensured the data quality operators, the ETL steps, and the create table steps were all successful.

  

### Data Updates

The data should be updated as follows;

* Immigrations: Daily if possible, since this data source will be changing rapidly. Updating this daily will make sure the data is up to date.
* Global Temperatures: Every month, since the records are available in monthly steps.
* Demographics: Every year, as this information will only update every year.
* Airport Codes: Every year, as this information will change very infrequently.



### Scenario Handling

#### Data Increased by 100x

If the data increased, I would use a higher spec redshift cluster. I set up a single node redshift cluster, and if the data was to increase by this quantity, we would require greater specifications to ensure the data was able to be queried quickly. I would also use spark for reading the input data, as the data increasing by this quantity would mean the ETL process would take a significant quanity of time.

#### Pipelines to be run at 7am

If the pipelines were required to be run at 7am every day, I would add an airflow schedule to enable this. This would then trigger the ETL pipeline to be run. I would add slack integrations, to notify users when the pipeline failed, such that relevant people could attend to the failure. I would also enable retries, to ensure that the failure wasn't due to servers being temporarily unavailable.

#### Database accessed by 100+ people

I would use a higher spec redshift cluster as well, to ensure the loading that would be placed on the database would be handled appropriately. I would add RBAC to the redshift database, to ensure that only certain users were able to perform operations that could comprise the database. I would write an API on top of the warehouse, such that users were able to query common queries easily, over rest. I would enable metrics, monitoring and alerting on the database, such that if there was any downtime, I would be able to attend to the issue.

 