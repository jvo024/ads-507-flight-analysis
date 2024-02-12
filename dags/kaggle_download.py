import os
from datetime import timedelta
import pandas as pd

from azure.storage.blob import BlobServiceClient
import opendatasets as od

def kaggle_download(dataset_url, file_name):

    print('Start Kaggle CSV download')
    
    # Download dataset using opendatasets
    od.download(dataset_url, force=True, quiet=False)
    
    # Define the directory path
    directory = os.path.join(os.getcwd(), 'raw_data')
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, file_name)
    
    # Read the CSV file
    csv_data = pd.read_csv(file_path)

    # Save the df to a new CSV
    csv_data.to_csv(file_path, index=False)

    print('End Kaggle CSV download')

# Variables for Kaggle Download
dataset_url = 'https://www.kaggle.com/datasets/tylerx/flights-and-airports-data'
file_name = 'flights-and-airports-data/airports.csv'

kaggle_download(dataset_url, file_name)