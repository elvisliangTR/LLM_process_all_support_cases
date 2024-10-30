# Data Processing Pipeline for GenAI processed results

This script outlines a data processing pipeline using Snowflake and a language model. Below are the key components and steps involved.

## Requirements:
- Snowflake Connector(https://github.com/elvisliangTR/_snowflake_connector): Required to load data from Snowflake. Alternatively, adjust the code for manually uploaded CSV files.
- Language Model (LLM): The script uses Claude 3.5. Note that the code will differ if using ChatGPT.

## Process Overview:
Data Extraction:
Load data from Snowflake using the Snowflake Connector.

Data Processing and Cleaning:
Conduct processing and cleaning in a Python Notebook.

DataFrame Creation:
Store the processed data in a Python Pandas DataFrame (DF).

Language Model Initialization:
Initialize the LLM to process the DataFrame in the Python Notebook.

DataFrame Update:
Append the processed result to a new column in the same DataFrame.

Data Writing:
Write the updated data back to Snowflake using the Snowflake Connector.

Business Intelligence Reporting:
BI reports fetch the data every day for analysis.

Notes:
Ensure all dependencies are installed and configured correctly.
Adjust the script as needed for different language models or data sources.
