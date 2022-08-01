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
from azure_data_lake import AzureDataLake, explore
from datetime import datetime


def datetime_to_epoch(date_time: datetime):
    return date_time.timestamp()


def epoch_to_datetime(epoch):
    return datetime.fromtimestamp(epoch)


def json_traversal(data, indent_level=0):
    data_type = type(data)
    indent = "\t" * (indent_level + 1)

    if indent_level == 0:
        print("/", end="")

    if data_type == list:
        # Check length
        print(f"{indent}list of len: {len(data)}")

        for item in data:
            json_traversal(item, indent_level + 1)
            break

    elif data_type == dict:
        # Print all keys
        for key in data.keys():
            
            print(f"{indent}{key}")

            json_traversal(data[key], indent_level + 1)

            # if str(data.keys()) == "dict_keys(['list', 'next_offset'])":
            #     explore(data["list"])
            # elif str(data.keys()) == "dict_keys(['subscription', 'customer'])":
            #     explore(data["subscription"])

            if key == "next_offset":
                print(data[key], type(data[key]))

        return


if __name__ == "__main__":
    # Load Environment Variables
    load_dotenv()
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    table_name = "events.json"
    "customers.json"
    "events.json"
    "invoices.json" # errored
    "orders.json" # pretty much empty
    "transctions.json"

    lake = AzureDataLake(connect_str)

    # Events data
    print("Downloading data from", table_name)

    eu_blob = lake.get_blob( 
        f"Unprocessed/Chargebee EU/2022/03/16/{table_name}"
    )

    try:
        storage_stream_downloader = eu_blob.download_blob()
    except ResourceNotFoundError:
        print("No blob found.")
        sys.exit()

    eu_json = json.loads(storage_stream_downloader.readall())

    # eu_df = lake.get_json_blob_as_df(
    #     blob_path=f"Unprocessed/Chargebee EU/2022/03/16/{table_name}"
    # )
    
    item = eu_json[0]["list"][0]["event"]
    item_type = type(item)

    print(item_type)

    if item_type == list:
        print(len(item))
    elif item_type == dict:
        pass
        print(item.keys())
    else:
        pass
        print(item)

    pprint(item)

    sys.exit()

    pprint(len(eu_json))

    first = eu_json[0]
    _type = type(first)
    print(first.keys())
    # print(first["list"])

    '''
    [
        {
            "list": [
                {
                    "subscription": {
                        'id', 
                        'plan_id', 
                        'plan_quantity', 
                        'plan_unit_price', 
                        'billing_period', 
                        'billing_period_unit', 
                        'trial_end', 
                        'customer_id', 
                        'plan_amount', 
                        'plan_free_quantity', 
                        'status', 
                        'trial_start', 
                        'created_at', 
                        'started_at', 
                        'cancelled_at', 
                        'cancel_reason', 
                        'updated_at', 
                        'has_scheduled_changes', 
                        'resource_version', 
                        'deleted', 
                        'object', 
                        'cancel_reason_code',
                        'currency_code',
                        'due_invoices_count',
                        'mrr', 
                        'channel'
                    },
                    "customer":  {
                        id
                                        first_name
                                        last_name
                                        email
                                        company
                                        auto_collection
                                        net_term_days
                                        allow_direct_debit
                                        created_at
                                        taxability
                                        updated_at
                                        pii_cleared
                                        resource_version
                                        deleted
                                        object
                                        card_status
                                        promotional_credits
                                        refundable_credits
                                        excess_payments
                                        unbilled_charges
                                        preferred_currency_code
                                        cf_account_url
                                        cf_gtmhub_account_status
                                        channel
                    }
                }
            ],
            "next_offset":
        },
    ]
    '''
    explore(eu_json)

    data = []
    headers = []
    first_time = True

    for row in eu_json:
        # row is a dict

        # dig into list key
        # _row = row["list"]

        for item in row["list"]:
            new_row = {}

            for prefix in item.keys():
                for key, value in item[prefix].items():
                    new_row[f"{prefix}.{key}"] = value
                    # if first_time:
                    #     headers.append(f"{key}.{}")
                    # new_row.append()
                    # print(prefix, key, value)
            data.append(new_row)
        # print(_row)
        # sys.exit()
    
    df = pd.DataFrame(data)

    print(df.head())

    print(df.columns)

    '''
    df.to_csv(
        os.path.join(
            "data",
            f"chargebee_{table_name}.csv"
        ), 
        index=False
    )
    '''