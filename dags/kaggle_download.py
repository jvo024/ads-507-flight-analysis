import os
from datetime import timedelta

from azure.storage.blob import BlobServiceClient
import opendatasets as od

import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator

# Credentials for Azure

azure_connection_string = 'DefaultEndpointsProtocol=https;AccountName=airplaneetl;AccountKey=K0pHJK8paR2yKLvxJKbz9nk7Fe4qsfsgMc/jZLYLtsXr+OTtyPVs8QcGYufvts3tJO2JYjRWKqWi+ASt71ofQw==;EndpointSuffix=core.windows.net'
container_name = 'airplanec'

def kaggle_download(dataset_url, file_name, azure_connection_string, container_name):

    print('Start Kaggle CSV download')
    
    # Download dataset using opendatasets
    od.download(dataset_url, force=True, quiet=False)
    
    # Upload the file to Azure Storage
    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Upload airports.csv
    with open(file_name, "rb") as data:
        container_client.upload_blob(name=os.path.basename(file_name), data=data)

    print('End Kaggle CSV download')

# Variables for Kaggle Download
dataset_url = 'https://www.kaggle.com/datasets/tylerx/flights-and-airports-data'
file_name = 'flights-and-airports-data/airports.csv'
azure_connection_string = azure_connection_string
container_name = container_name

kaggle_csv_download(dataset_url, file_name, azure_connection_string, container_name)