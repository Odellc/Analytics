from operator import index
import os
import sys
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from dotenv import load_dotenv
import pandas as pd
import datetime
from dateutil import parser as date_parser
import json
from pprint import pprint
from functions_2 import *

# Environment vars
load_dotenv()
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

# Hardcoded date
today = datetime.datetime(year=2022, month=4, day=7)
today_str = datetime_string(today, "/")

# Instantiate blob service client
try:
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
except Exception as e:
    print(f'Unable to connect to BlobServiceClient: {e}')

# Fetching users, accounts, and chargebee subscriptions.
# Merging all tables into one.
df = get_users_accounts_subscriptions_data(blob_service_client, today_str)

partners_df = df[df["account|type"] == 4.0]

print("# of partners:", partners_df.shape)

partners_df.to_csv("partners.csv", index=False)