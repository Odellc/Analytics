#!/usr/bin/env python
# coding: utf-8

'''
Steps:
1. Get all event data for the events specified.
2. Save entire file to csv
3. Count the number of times the event is triggered per month for each user.
4. Join user information.
5. Join account information.
6. Join chargebee information to account.
7. Save aggregated file to csv

Project: Starting Variables for Analyzing Expansion Behavior
Author: Tavon Pourboghrat
'''
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

from config import event_table_names

REGEX_JSON_PATTERN = r"([{].*?[}])"


def get_date_str(date_and_time):
    year = str(date_and_time.year)
    month = str(date_and_time.month).zfill(2)
    day = str(date_and_time.day).zfill(2)

    return f"{year}/{month}/{day}"


def get_full_blob_name(top_level_dir, data_source, blob_name: str, date_and_time: datetime):
    return f"{top_level_dir}/{data_source}/{get_date_str(date_and_time)}/{blob_name}"


def get_json_blob_as_df(blob_service_client, container, blob_path):
    """
    Get a JSON blob from Azure and read it into a pandas dataframe.
    Params:
        :blob_service_client (BlobServiceClient object): Azure blob service client object
        :container (str): Name of the Azure storage container
        :blob_path (str): Name of the Azure blob
    """
    blob_client = blob_service_client.get_blob_client(container, blob_path)

    try:
        storage_stream_downloader = blob_client.download_blob()
    except ResourceNotFoundError:
        print("No blob found.")
        return None

    file_reader = json.loads(storage_stream_downloader.readall())
    
    df = pd.DataFrame(file_reader)

    return df


def get_stats(df):
    users = df["user_id"].unique().tolist()

    data = []

    for user in users:
        _df = df[
            df["gtmhub_application_name"] != "website"
        ]

        num_rows = _df.shape[0]

        data.append([user, num_rows])

    return pd.DataFrame(data)


def get_accounts(blob_service_client, date_time):
    eu_accounts = get_json_blob_as_df(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawaccountseu.json"
    )

    us_accounts = get_json_blob_as_df(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawaccountsus.json"
    )

    # Combine account dfs
    accounts_df = pd.concat([eu_accounts, us_accounts])

    return accounts_df


def get_users(blob_service_client, date_time):
    eu_users = get_json_blob_as_df(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawuserseu.json"
    )

    us_users = get_json_blob_as_df(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawusersus.json"
    )

    # Combine user dfs
    users_df = pd.concat([eu_users, us_users])

    return users_df


def json_to_ordered_list(json_object, headers):
    return [
        json_object[header]
        for header in headers
    ]


def get_user_events_by_month():
    pass


def get_params():
    save_full = True if "save-full" in sys.argv else False
    save_condensed = True if "save-condensed" in sys.argv else False
        
    return save_full, save_condensed


def read_large_json_by_row(blob_client):
    try:
        storage_stream_downloader = blob_client.download_blob()
    except ResourceNotFoundError:
        print("No blob found.")
        return None

    headers = []
    from_previous_chunk = ""

    for chunk_index, chunk in enumerate(storage_stream_downloader.chunks()):
        chunk_str = chunk.decode("utf-8")

        lines = chunk_str.split("\n")

        if from_previous_chunk != "":
            lines[0] = from_previous_chunk + lines[0]

        for line_index, line in enumerate(lines):
            # print(line)

            # If last line in the chunk, check if the object is complete
            if line_index == len(lines) - 1:
                if line[-1] == "}":
                    print("all good")
                    from_previous_chunk = ""
                else:
                    print("incomplete")
                    from_previous_chunk = line
                    continue

            # Clean up line variable and convert to json
            regex_result = re.search(REGEX_JSON_PATTERN, line).group(1)

            try:
                json_row_data = json.loads(regex_result)
            except Exception as e:
                print("Error: {e}")
                print("\t", regex_result)
            
            # print(line)
            # print()
            # print(regex_result)
            # print()
            # pprint(json_row_data)
            
            # If first chunk and first line, write headers
            if chunk_index == 0 and line_index == 0:
                headers = sorted(json_row_data.keys())
                row_data = headers
            else:
                # Convert json to list
                row_data = json_to_ordered_list(json_row_data, headers)

            full_data_path = os.path.join("data", "full", file_name + ".csv")
            append_row_to_csv(full_data_path, row_data)


def append_row_to_csv(csv_path, row_data):
    with open(csv_path, "a", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)

        try:
            csv_writer.writerow(row_data)
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    # Load Environment Variables
    load_dotenv()
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    save_full, save_condensed = get_params()

    # Choose a specific date to ensure consistent data pulls
    date_and_time = datetime.datetime(year=2022, month=2, day=28)

    # Instantiate blob service client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    except Exception as e:
        print(f'Unable to connect to BlobServiceClient: {e}')

    # Get users
    users_df = get_users(blob_service_client, date_and_time)
    # Remove unneeded columns
    users_df = users_df.drop(['clientid', 'additionalinvitationsleft', 'data_source_id', 'sync_date'], axis=1)
    # Remove non-english and inactive users
    # users_df = users_df[(users_df['isactive'] == True)] # ToDo: add a column designating is_active
    # Remove users if they were created less that 60 days ago
    users_df['datecreated'] = pd.to_datetime(users_df['datecreated'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')
    six_months_ago = datetime.datetime.today() - datetime.timedelta(days=60) # start of 2021
    users_df = users_df[users_df['datecreated'] < six_months_ago]
    # Add prefixes for table clarity
    users_df = users_df.add_prefix('user_')

    # Get accounts
    accounts_df = get_accounts(blob_service_client, date_and_time)
    # Remove unnecessary columns
    accounts_df = accounts_df.drop(['ownerid', 'edition', 'planid', 'settings', 'data_source_id', 'sync_date'], axis=1)
    # Keep active accounts
    # accounts_df = accounts_df[accounts_df['isactive'] == True]
    # Fix datetime
    accounts_df['datecreated'] = pd.to_datetime(accounts_df['datecreated'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')
    # Add prefixes for table clarity
    accounts_df = accounts_df.add_prefix('account_')
    print(users_df)    

    event_data = {}
    combined_data = None

    for table_name in event_table_names:
        full_table_name = get_full_blob_name(
            "Unprocessed",
            "Redshift",
            table_name,
            date_and_time
        )
        file_name = table_name.split(".")[0]

        print(file_name)

        blob_client = blob_service_client.get_blob_client("researchanalyticsinsights", full_table_name)

        # Blob size in Gigabytes (Gb)
        # Conversion seen here: https://gbmb.org
        blob_size = blob_client.get_blob_properties().size / (2 ** 30)
        print(f"\tBlob size: {blob_size:.2f} Gb")

        if blob_size >= 20:
            continue
            try:
                storage_stream_downloader = blob_client.download_blob()
            except ResourceNotFoundError:
                print("No blob found.")
                sys.exit()

            headers = []
            from_previous_chunk = ""

            for chunk_index, chunk in enumerate(storage_stream_downloader.chunks()):
                chunk_str = chunk.decode("utf-8")

                lines = chunk_str.split("\n")

                if from_previous_chunk != "":
                    lines[0] = from_previous_chunk + lines[0]

                for line_index, line in enumerate(lines):
                    # print(line)

                    # If last line in the chunk, check if the object is complete
                    if line_index == len(lines) - 1:
                        if line[-1] == "}":
                            print("all good")
                            from_previous_chunk = ""
                        else:
                            print("incomplete")
                            from_previous_chunk = line
                            continue

                    # Clean up line variable and convert to json
                    regex_result = re.search(REGEX_JSON_PATTERN, line).group(1)

                    try:
                        json_row_data = json.loads(regex_result)
                    except Exception as e:
                        print("Error: {e}")
                        print("\t", regex_result)
                    
                    # print(line)
                    # print()
                    # print(regex_result)
                    # print()
                    # pprint(json_row_data)
                    
                    # If first chunk and first line, write headers
                    if chunk_index == 0 and line_index == 0:
                        headers = sorted(json_row_data.keys())
                        row_data = headers
                    else:
                        # Convert json to list
                        row_data = json_to_ordered_list(json_row_data, headers)

                    full_data_path = os.path.join("data", "full", file_name + ".csv")
                    with open(full_data_path, "a", newline="") as csv_file:
                        csv_writer = csv.writer(csv_file)

                        try:
                            csv_writer.writerow(row_data)
                        except Exception as e:
                            print(f"Error: {e}")
        else:
            print(f"Converting {full_table_name} to dataframe...")

            df = get_json_blob_as_df(
                blob_service_client,
                "researchanalyticsinsights",
                full_table_name
            )

            # print(event_data[full_table_name].head())

            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                infer_datetime_format=True
            )

            df["month"] = df["timestamp"].map(lambda x: str(x.month).zfill(2))
            df["year"] = df["timestamp"].map(lambda x: str(x.year))
            df["year_month"] = df["year"].str.cat(df["month"], sep="-")

            print(df["year_month"].head())
            
            if save_full:
                df.to_csv(
                    os.path.join("data", "full", file_name + ".csv"),
                    index=False
                )

            '''
            for row in df.itertuples(index=False):
                user = getattr(row, "user_id")
                year_month = getattr(row, "year_month")

                if user not in event_data.keys():
                    event_data[user] = {}

                if year_month not in event_data[user].keys():
                    event_data[user][year_month] = {}

                if file_name not in event_data[user][year_month].keys():
                    event_data[user][year_month][file_name] = 0
                
                event_data[user][year_month][file_name] += 1
            '''

            num_events_by_group_df = df.groupby(["user_id", "year", "month"]).size().to_frame(name=f"{file_name}.count")

            print(num_events_by_group_df)

            if combined_data is None:
                combined_data = num_events_by_group_df
            else:
                print("Join")
                print(combined_data)
                combined_data = combined_data.join(num_events_by_group_df, on=["user_id", "year", "month"])
                print(combined_data)

            # users = df["user_id"].unique().tolist()

            # for user in users:
                

            #     user_data = df[df["user_id"] == user]


            # user_events_by_month = map(get_user_events_by_month, users)

            # stats_by_user = data.groupby('user_id')['id'].count().reset_index().rename(columns={'id': f'{file_name}.num_events'})
            # # stats_by_user.columns = ["user", f"number_of_{file_name}"]

            # # print("data:")
            # # print(stats_by_user.head())

            # # Save aggregated data to file
            # print("\t" + f"{file_name}: saving...")
            # stats_by_user.to_csv(
            #     os.path.join("data", "condensed", file_name + ".csv"),
            #     index=False
            # )

    print("The next two")
    pprint(event_data)
    print(combined_data)

    # Convert dictionary -> nested list -> DataFrame
    # data = []
    # headers = ["user_id", "year", "month"]
    # for user_id, time_periods in event_data.items():
    #     for time_period, events in time_periods.items():
    #         for event, count in events.items():

    #             data.append([
    #                 user_id,
    #                 time_periods
    #             ])

    # print(pd.DataFrame(event_data))

    df = combined_data.merge(users_df, how="left", left_on="user_id", right_on="user_id")

    print(df.head())

    df.to_csv(
        os.path.join(
            "data",
            "condensed",
            "event_data.csv"
        ), 
        index=False
    )

    sys.exit()
    
    

    # Get number of users per account
    account_sum = accounts_df.merge(users_df, how='inner', left_on='account_id', right_on='user_accountid')
    account_sum = account_sum.groupby('account_id')['user_id'].count().reset_index().rename(columns={'user_id': 'user_count'})

    print("added number of users per account")

    # Get backend users
    backendusers_df = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendusers.json")

    print("get backend schema data")

    # backendusers_df.info()

    # Remove unnecessary columns
    backendusers_df = backendusers_df.drop(['received_at', 'uuid', 'editionname', 'account_id', 'account_name', 'accountstatus', 'avatar', 'context_library_name', 'company_account_status', 'uuid_ts', 'accountcreated', 'company_id', 'context_library_version', 'trialends', 'company_plan', 'created_at', 'editionplanid', 'context_group_id', 'deleted', 'company_status', 'status', 'company_edition', 'experiments', 'is_primary'], axis=1)

    # Add prefix
    backendusers_df = backendusers_df.add_prefix('backenduser_')

    # Join backend with users
    user_df = users_df.merge(backendusers_df, how='inner', left_on='user_id', right_on='backenduser_id')

    # Join sum with accounts
    account_df = accounts_df.merge(account_sum, how='inner', left_on='account_id', right_on='account_id')

    # Drop additional user columns
    user_df = user_df.drop(['user_isactive', 'backenduser_id', 'backenduser_company_created_at', 'backenduser_last_name', 'backenduser_first_name', 'backenduser_roles', 'user_language'], axis=1)
    # Drop additional account columns
    account_df = account_df.drop(['account_language', 'account_isactive'], axis=1)

    # Join users and accounts
    df = user_df.merge(account_df, how='inner', left_on='user_accountid', right_on='account_id')

    # Drop duplicate column
    df = df.drop(['user_accountid'], axis=1)

    print("df:")
    print(df.head())

    # Get task information
    task_created = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendtask_created.json")
    task_deleted = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendtask_deleted.json")
    task_modified = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendtask_modified.json")

    # Create task_created by user df
    task_created = task_created.groupby('user_id')['id'].count().reset_index().rename(columns={'id': 'tasks_created'})
    task_created.head()

    # Create task_deleted by user df
    task_deleted = task_deleted.groupby('user_id')['id'].count().reset_index().rename(columns={'id': 'tasks_deleted'})
    task_deleted.head()

    # Create task_modified by user df
    task_modified = task_modified.groupby('user_id')['id'].count().reset_index().rename(columns={'id': 'tasks_modified'})
    task_modified.head()

    # Merge task_* dfs
    tasks_df = task_created.merge(task_deleted, how='outer', left_on='user_id', right_on='user_id')
    tasks_df = tasks_df.merge(task_modified, how='outer', left_on='user_id', right_on='user_id')

    # NaN to 0
    tasks_df = tasks_df.fillna(0)

    # Merge tasks with users
    df = df.merge(tasks_df, how='inner', left_on='user_id', right_on='user_id')

    # Remove duplicate rows
    df = df[~df.duplicated(keep='last')]

    print("Fetching hubspot data...")

    # Get HubSpot Contacts
    hs_contacts = pd.read_json('hubspot_contacts.json')
    # Explode properties
    hs_contacts = hs_contacts.join(hs_contacts.properties.apply(pd.Series))
    # Keep necessary columns
    hs_contacts = hs_contacts[['associatedcompanyid', 'email', 'hs_object_id', 'jobtitle']]

    # Get HubSpot Companies
    hs_companies = pd.read_json('hubspot_companies.json')
    # Explode properties
    hs_companies = hs_companies.join(hs_companies.properties.apply(pd.Series))
    # Keep necessary columns
    hs_companies = hs_companies[['hs_object_id', 'annualrevenue', 'industry', 'numberofemployees', 'website']]

    # Merge hubspot contacts with companies 
    hubspot = hs_contacts.merge(hs_companies, how='left', left_on='associatedcompanyid', right_on='hs_object_id')
    hubspot = hubspot.drop(['associatedcompanyid', 'hs_object_id_x', 'hs_object_id_y'], axis=1)

    # hubspot.head()
    print("\tdone")

    # Subsription SQL
    # ```
    # SELECT
    #     subscription_id,
    #     subscription_mrr
    # FROM chargebee_rest_subscriptions_all
    # WHERE subscription_id IN (<list-of-subscription-ids-from-df>)
    # ORDER BY subscription_id
    # ```

    print("Reading subscription data...")
    # Get subscriptions
    subscriptions = pd.read_csv('subscriptions.csv')
    # Remove unnecessary columns
    subscriptions = subscriptions.drop(['Unnamed: 0'], axis=1)

    # subscriptions.head()

    # Merge subscriptions with users
    df = df.merge(subscriptions, how='inner', left_on='account_subscriptionid', right_on='subscription_id')
    # Remove $0 subscriptions
    df = df[df.subscription_mrr > 0]
    # Remove gtmhub & primeholding users
    df = df[~df.backenduser_email.str.contains('primeholding')]
    df = df[~df.backenduser_email.str.contains('gtmhub')]
    # Remove unnecessary columns
    df = df.drop(['account_subscriptionid', 'subscription_id', 'subscription_mrr', 'user_id'], axis=1)

    # Merge users with HubSpot data
    df = df.merge(hubspot, how='left', left_on='backenduser_email', right_on='email')

    # Write user sample to CSV
    df.to_csv(
        f"event_data_as_of_{get_date_str(date_and_time)}.csv", 
        index=False
    )
