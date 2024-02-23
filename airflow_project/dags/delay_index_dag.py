import os
from airflow.decorators import dag, task
from pendulum import datetime
from dotenv import load_dotenv
import requests
import json

load_dotenv()

@dag(
    schedule="@daily",
    start_date=datetime(2024, 2, 22),
    catchup=False,
)
def run_delay_index_api():

    @task()
    def delay_index_call():
        print('Start API call')
        
        flightstats_app_id = os.getenv("flightstats_app_id")
        flightstats_app_key = os.getenv("flightstats_app_key")

        url = f"https://api.flightstats.com/flex/delayindex/rest/v1/json/country/US?appId={flightstats_app_id}&appKey={flightstats_app_key}"

        response = requests.get(url)

        if response.status_code == 200:
            new_json_data = response.json()

            directory = os.path.join(os.getcwd(), 'raw_data')
            if not os.path.exists(directory):
                os.makedirs(directory)

            file_name = "delay_index.json"
            file_path = os.path.join(directory, file_name)

            existing_json_data = {}
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    existing_json_data = json.load(json_file)

            existing_json_data.update(new_json_data)

            with open(file_path, 'w', encoding='utf-8') as json_file:
                json.dump(existing_json_data, json_file, ensure_ascii=False, indent=4)

            print('End API call')

        else:
            print(f"Error: Unable to fetch data. Status code: {response.status_code}")

    delay_index_call()

run_delay_index_api()
