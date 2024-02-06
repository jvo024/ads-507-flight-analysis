import json
import requests
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import datetime

start_date = airflow.utils.dates.days_ago(2)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

def icao_api_scraper(api_key, state_of_operator, file_name, container_name, azure_storage_connection_string):
    print('Start run')
    
    # Define the ICAO API URL
    url = f"https://applications.icao.int/dataservices/api/any?api_key={api_key}&StateOfOccurrence=&format=json&StateOfOperator={state_of_operator}&StateOfRegistry=&Year="

    # Make the API call
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        json_data = response.json()

        # Save the JSON data to a file
        with open(file_name, 'w', encoding='utf') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)

        print('End run')

        # Upload the file to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        with open(file_name, "rb") as data:
            container_client.upload_blob(name=file_name, data=data)

    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")

icao_api_scraper('a74b3990-66e7-44f8-ad40-0dc664d3912a','USA','icao_api_scrape.json','airplanec',
'DefaultEndpointsProtocol=https;AccountName=airplaneetl;AccountKey=K0pHJK8paR2yKLvxJKbz9nk7Fe4qsfsgMc/jZLYLtsXr+OTtyPVs8QcGYufvts3tJO2JYjRWKqWi+ASt71ofQw==;EndpointSuffix=core.windows.net')

with DAG(
    "icao_api_scrape",
    default_args = default_args,
    description = 'Call ICAO API for safety related occurrences with flights operating from the USA',
    schedule = None,
    start_date = start_date,
    catchup = False,
    tags = ["ads507"],
) as dag:

    extract_icao = PythonOperator(
        task_id = 'extract_icao',
        python_callable = icao_api_scraper,
        op_kwargs = {
            'api_key': 'a74b3990-66e7-44f8-ad40-0dc664d3912a',
            'state_of_operator': 'USA',
            'file_name': 'icao_api_scrape.json',
            'container_name': 'airplanec',
            'azure_storage_connection_string': 'DefaultEndpointsProtocol=https;AccountName=airplaneetl;AccountKey=K0pHJK8paR2yKLvxJKbz9nk7Fe4qsfsgMc/jZLYLtsXr+OTtyPVs8QcGYufvts3tJO2JYjRWKqWi+ASt71ofQw==;EndpointSuffix=core.windows.net'},
        dag = dag
        )
    
    data = EmptyOperator(task_id = 'data', dag=dag)

    extract_icao >> data



    