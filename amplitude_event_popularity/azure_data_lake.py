from tokenize import group
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from dotenv import load_dotenv
from numpy import savez_compressed
import pandas as pd
import datetime
import json
import os
import sys
from pprint import pprint
import csv
import re

REGEX_JSON_PATTERN = r"([{].*?[}])"
ROOT_CONTAINER = "researchanalyticsinsights"


class AzureDataLake:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.blob_service_client = self._connect()

    def _connect(self):
        # Instantiate blob service client
        try:
            return BlobServiceClient.from_connection_string(self.connection_string)
        except Exception as e:
            print(f'Unable to connect to BlobServiceClient: {e}')
            return None

    def get_blob(self, path):
        """
        Get a JSON blob from Azure and read it into a pandas dataframe.
        Params:
            :blob_service_client (BlobServiceClient object): Azure blob service client object
            :container (str): Name of the Azure storage container
            :blob_path (str): Name of the Azure blob
        """
        blob_client = self.blob_service_client.get_blob_client(ROOT_CONTAINER, path)

        return blob_client

    def get_full_blob_name(self, top_level_dir, data_source, blob_name: str, date_and_time: datetime):
        # return f"{top_level_dir}/{data_source}/{get_date_str(date_and_time)}/{blob_name}"
        pass

    def get_json_blob_as_df(self, blob_path, container=ROOT_CONTAINER):
        """
        Get a JSON blob from Azure and read it into a pandas dataframe.
        Params:
            :blob_service_client (BlobServiceClient object): Azure blob service client object
            :container (str): Name of the Azure storage container
            :blob_path (str): Name of the Azure blob
        """
        blob_client = self.blob_service_client.get_blob_client(container, blob_path)

        try:
            storage_stream_downloader = blob_client.download_blob()
        except ResourceNotFoundError:
            print("No blob found.")
            return None

        file_reader = json.loads(storage_stream_downloader.readall())
        
        df = pd.DataFrame(file_reader)

        return df


def explore(data, indent_level=0, new_data=[]):
    data_type = type(data)
    indent = "\t" * (indent_level + 1)

    if indent_level == 0:
        print("/", end="")

    if data_type == list:
        # Check length
        print(f"{indent}list of len: {len(data)}")

        # Get first element
        explore(data[0], indent_level + 1)

    elif data_type == dict:
        # Print all keys
        for key in data.keys():
            
            print(f"{indent}{key}")

            explore(data[key], indent_level + 1)

            # if str(data.keys()) == "dict_keys(['list', 'next_offset'])":
            #     explore(data["list"])
            # elif str(data.keys()) == "dict_keys(['subscription', 'customer'])":
            #     explore(data["subscription"])

            if key == "next_offset":
                print(data[key], type(data[key]))

        return