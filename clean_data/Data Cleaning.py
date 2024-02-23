#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from sqlalchemy import create_engine
import json
import sqlite3

airports=pd.read_csv('airports.csv')


# In[3]:


# Data cleaning steps for CSV containing airport ID and location

# Remove duplicate rows based on the ID
airports = airports.drop_duplicates(subset='airport_id', keep='first')

# Drop rows with missing values
airports = airports.dropna()

# Strip leading and trailing whitespaces from all string columns
airports = airports.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)

# Display the cleaned DataFrame
print("\nCleaned DataFrame:")
print(airports)

# Save the cleaned DataFrame back to a new CSV file
airports.to_csv('cleaned_airports.csv', index=False)


# In[4]:


with open('airline_operator_risk.json', 'r') as json_file:
    data = json.load(json_file)

# Convert JSON data to DataFrame
df_airline = pd.json_normalize(data)

# Display  original DataFrame
print("Original DataFrame:")
print(df_airline)

# Data cleaning steps
# Drop rows with missing values
df_airline = df_airline.dropna()

# Strip leading and trailing whitespaces from string columns
df_airline = df_airline.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)

# Perform an SQL transformation (example: filtering based on a condition)
conn_airline = sqlite3.connect(':memory:')  # Create an in-memory SQLite database
with conn_airline:
    df_airline.to_sql('airline_data', conn_airline, index=False, if_exists='replace')  # Load DataFrame into SQLite table

# Example SQL query: Select rows where av_fleet_age is greater than 10
sql_query_airline = "SELECT * FROM airline_data WHERE av_fleet_age > 10"
df_filtered_airline = pd.read_sql(sql_query_airline, conn_airline)

# Display the cleaned DataFrame after the SQL transformation
print("\nCleaned DataFrame after SQL transformation:")
print(df_filtered_airline)

# Save the cleaned DataFrame back to a new JSON file
df_filtered_airline.to_json('cleaned_airline_data.json', orient='records', lines=True)


# In[5]:


with open('airline_incidents.json', 'r') as json_file:
    data = json.load(json_file)

# Convert JSON data to DataFrame
df_incidents = pd.json_normalize(data)

# Display the original DataFrame
print("Original DataFrame:")
print(df_incidents)

# Data cleaning steps
# Drop rows with missing values
df_incidents = df_incidents.dropna()

# Strip leading and trailing whitespaces from string columns
df_incidents = df_incidents.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)

# Perform an SQL transformation (example: filtering based on a condition)
conn_incidents = sqlite3.connect(':memory:')  # Create an in-memory SQLite database
with conn_incidents:
    df_incidents.to_sql('incidents_data', conn_incidents, index=False, if_exists='replace')  # Load DataFrame into SQLite table

# Example SQL query: Select rows where FlightPhase is 'Approach'
sql_query_incidents = "SELECT * FROM incidents_data WHERE FlightPhase = 'Approach'"
df_filtered_incidents = pd.read_sql(sql_query_incidents, conn_incidents)

# Display the cleaned DataFrame after the SQL transformation
print("\nCleaned DataFrame after SQL transformation:")
print(df_filtered_incidents)

# Save the cleaned DataFrame back to a new JSON file
df_filtered_incidents.to_json('cleaned_incidents_data.json', orient='records', lines=True)


# In[12]:


with open('delay_index.json', 'r') as json_file:
    data = json.load(json_file)

# Flatten the nested JSON structure
df_delay_indexes = pd.json_normalize(data['delayIndexes'])

# Display the original DataFrame
print("Original DataFrame:")
print(df_delay_indexes)

# Data cleaning steps
# Drop rows with missing values
df_delay_indexes = df_delay_indexes.dropna()

# Strip leading and trailing whitespaces from string columns
df_delay_indexes = df_delay_indexes.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)

# Display the cleaned DataFrame
print("\nCleaned DataFrame:")
print(df_delay_indexes)

# Save the cleaned DataFrame back to a new JSON file
df_delay_indexes.to_json('cleaned_delay_indexes.json', orient='records', lines=True)


# In[8]:


import json
import pandas as pd
import sqlite3

# Load data from the JSON file
with open('airline_accidents.json', 'r') as json_file:
    data = json.load(json_file)

# Convert JSON data to DataFrame
df_accidents = pd.json_normalize(data)

# Display the original DataFrame
print("Original DataFrame:")
print(df_accidents)

# Data cleaning steps
# Drop rows with missing values
df_accidents = df_accidents.dropna()

# Strip leading and trailing whitespaces from string columns
df_accidents = df_accidents.apply(lambda x: x.str.strip() if x.dtype == 'O' else x)

# Perform any additional data cleaning steps if needed

# Perform an SQL transformation (example: filtering based on a condition)
# Note: Replace 'YourColumnName' with the actual column you want to filter on
conn_accidents = sqlite3.connect(':memory:')  # Create an in-memory SQLite database
with conn_accidents:
    df_accidents.to_sql('accident_data', conn_accidents, index=False, if_exists='replace')  # Load DataFrame into SQLite table

# Example SQL query: Select rows where Fatalities is greater than or equal to 0
sql_query_accidents = "SELECT * FROM accident_data WHERE CAST(Fatalities AS INTEGER) >= 0"
df_filtered_accidents = pd.read_sql(sql_query_accidents, conn_accidents)

# Display the cleaned DataFrame after the SQL transformation
print("\nCleaned DataFrame after SQL transformation:")
print(df_filtered_accidents)

# Save the cleaned DataFrame back to a new JSON file
df_filtered_accidents.to_json('cleaned_airline_accidents.json', orient='records', lines=True)


# In[ ]:




