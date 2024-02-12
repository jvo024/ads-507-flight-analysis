import json
import os
import requests
import datetime
from datetime import timedelta

from dotenv import load_dotenv
load_dotenv()

def operator_risk_call(api_key, file_name):

    print('Start API call')
    
    url = f"https://applications.icao.int/dataservices/api/profile-stats?api_key={api_key}&states=usa&format=json&operators="

    # Make the API call
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        json_data = response.json()

        # Define the directory path
        directory = os.path.join(os.getcwd(), 'raw_data')
        if not os.path.exists(directory):
            os.makedirs(directory)

        file_path = os.path.join(directory, file_name)

        # Save the JSON data to the file path
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)

        print('End API call')

    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")

def accidents_call(api_key, file_name):

    print('Start API call')
    
    url = f"https://applications.icao.int/dataservices/api/accidents?api_key={api_key}&StateOfOccurrence=&format=json&StateOfOperator=usa&StateOfRegistry=&Year="

    # Make the API call
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        json_data = response.json()

        # Define the directory path
        directory = os.path.join(os.getcwd(), 'raw_data')
        if not os.path.exists(directory):
            os.makedirs(directory)

        file_path = os.path.join(directory, file_name)

        # Save the JSON data to the file path
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)

        print('End API call')

    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")

# Variables        
icao_api_key = os.getenv("icao_api_key")

operator_risk_file_name = 'airline_operator_risk.json'
accidents_file_name = 'airline_accidents.json'

operator_risk_call(icao_api_key, operator_risk_file_name)
accidents_call(icao_api_key, accidents_file_name)