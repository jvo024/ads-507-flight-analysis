import json
import os
import requests
import datetime
from datetime import timedelta

from dotenv import load_dotenv
load_dotenv()

def delay_index_call(app_id, app_key, file_name):

    print('Start API call')
    
    url = f"https://api.flightstats.com/flex/delayindex/rest/v1/json/country/US?appId={app_id}&appKey={app_key}"

    # Make the API call
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        new_json_data = response.json()

        # Define the directory path
        directory = os.path.join(os.getcwd(), 'raw_data')
        if not os.path.exists(directory):
            os.makedirs(directory)

        file_path = os.path.join(directory, file_name)

        existing_json_data = {}
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as json_file:
                existing_json_data = json.load(json_file)
        
        existing_json_data.update(new_json_data)

        # Save the JSON data to the file path
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(existing_json_data, json_file, ensure_ascii=False, indent=4)

        print('End API call')

    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")

flightstats_app_id = os.getenv("flightstats_app_id")
flightstats_app_key = os.getenv("flightstats_app_key")

delay_index_file_name = "delay_index.json"

delay_index_call(flightstats_app_id, flightstats_app_key, delay_index_file_name)
