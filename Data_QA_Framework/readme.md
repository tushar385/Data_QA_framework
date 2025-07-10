# Data Quality Framework

## Overview
This framework is designed to perform data quality checks and generate reports for various data sources using Snowflake. It includes functionalities for exploratory data analysis (EDA) and month-over-month (MoM) validation.

## Setup Instructions

### 1. Update Snowflake Credentials
Before running the framework, you need to update your Snowflake credentials. Open the `snowflake_config.py` file located in the `configs` directory and replace the placeholder values with your actual Snowflake account details.


### 2. Add Your Required Tables
You need to specify the tables you want to analyze. Open the `tables_list.txt` file located in the root directory and add your table names in the specified format.


### 3. Call the Data Quality Functions
To execute the data quality checks, you can use the Jupyter Notebook provided in the `stats.ipynb` file. This notebook allows you to call the necessary functions for EDA and MoM validation.



### 4. Results Storage
The results of the data quality checks and analyses are stored in the `artifacts` directory. You can find the output CSV files there after running the functions.

## Additional Information
- Ensure you have the necessary permissions to access the Snowflake tables specified in your `tables_list.txt`.
- The framework uses various utility functions for JSON formatting and CSV dumping, which are located in the `utils` directory.

