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

The jupyter notebook is available in `./UdacityDendCapstone.ipynb`
