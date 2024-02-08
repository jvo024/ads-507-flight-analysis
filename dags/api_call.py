import json
import requests

from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient

import airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import datetime
import pendulum


# Credentials for Azure

azure_connection_id = 'azure-data-factory'
factory_name = 'airplane-df'
resource_group_name = 'ads507-team6'

# Python Operator for ICAO API Call

icao_api_key = 'a74b3990-66e7-44f8-ad40-0dc664d3912a'
icao_state_of_operator = 'USA'
icao_file_name = 'icao_api_scrape.json'

def icao_api_call(api_key, state_of_operator, file_name, container_name, azure_connection_string):
    print('Start ICAO API call')
    
    url = f"https://applications.icao.int/dataservices/api/any?api_key={api_key}&StateOfOccurrence=&format=json&StateOfOperator={state_of_operator}&StateOfRegistry=&Year="

    # Make the API call
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        json_data = response.json()

        # Save the JSON data to a file
        with open(file_name, 'w', encoding='utf') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)
        
        # Upload the file to Azure Storage
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        with open(file_name, "rb") as data:
            container_client.upload_blob(name=file_name, data=data)

        print('End ICAO API call')

    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")

# Function for Open Sky API Call
        

# Function for Kaggle API Call
        

# Function for Tomorrow.io API Call


# DAGs  
start_date = pendulum.today('UTC').add(days=-2)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    dag_id="api_call_pipeline",
    start_date=datetime(2024, 2, 7),
    schedule_interval="@weekly",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3),
        "azure_data_factory_conn_id": azure_connection_id,
        "factory_name": factory_name,
        "resource_group_name": resource_group_name,  
    },
    default_view="graph",
) as dag:

    icao_api_call = PythonOperator(
        task_id = 'icao_api_call',
        python_callable = icao_api_call,
        op_kwargs = {
            'api_key': icao_api_key,
            'state_of_operator': icao_state_of_operator,
            'file_name': icao_file_name,
            'container_name': container_name,
            'azure_connection_string': azure_connection_string},
        dag = dag
        )
    
    ready = EmptyOperator(task_id='ready')
    
    # Set task dependencies
    icao_api_call >> ready