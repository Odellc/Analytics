#!/usr/bin/env python
# coding: utf-8

"""
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
"""

from calendar import month
from tokenize import group
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import (
    BlobServiceClient,
    BlobClient,
    ContainerClient,
    __version__,
)
from dotenv import load_dotenv
from numpy import savez_compressed
import pandas as pd
from datetime import datetime
import json
import os
import sys
from pprint import pprint
import csv
import re
from dateutil import parser as date_parser
import traceback

from config import event_table_names

DEFAULT_AZURE_CONTAINER = "researchanalyticsinsights"
REGEX_JSON_PATTERN = r"({.*?(?:{[^}]*?})*.*?})"
# r'([{] [^}{]+ [{]? [^}{]+ [}])'
# r'\{(?:[^}{]+|\{(?:[^}{]+|\{[^}{}]*\})*\})*\}'
# r'([{].*?[}]})$'
# r"([{].*?[}])"


def get_date_str(date_and_time):
    year = str(date_and_time.year)
    month = str(date_and_time.month).zfill(2)
    day = str(date_and_time.day).zfill(2)

    return f"{year}/{month}/{day}"


def get_full_blob_name(
    top_level_dir, data_source, blob_name: str, date_and_time: datetime
):
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
        _df = df[df["gtmhub_application_name"] != "website"]

        num_rows = _df.shape[0]

        data.append([user, num_rows])

    return pd.DataFrame(data)


def get_accounts(blob_service_client, date_time):
    eu_accounts = get_json_blob_as_df(
        blob_service_client,
        "researchanalyticsinsights",
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawaccountseu.json",
    )

    us_accounts = get_json_blob_as_df(
        blob_service_client,
        "researchanalyticsinsights",
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawaccountsus.json",
    )

    # Combine account dfs
    accounts_df = pd.concat([eu_accounts, us_accounts])

    return accounts_df


def get_users(blob_service_client, date_time):
    eu_users = get_json_blob_as_df(
        blob_service_client,
        "researchanalyticsinsights",
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawuserseu.json",
    )

    us_users = get_json_blob_as_df(
        blob_service_client,
        "researchanalyticsinsights",
        f"Unprocessed/Gtmhub/{get_date_str(date_time)}/gtmhubrawusersus.json",
    )

    # Combine user dfs
    users_df = pd.concat([eu_users, us_users])

    return users_df


def json_to_ordered_list(json_object, headers):
    return [json_object[header] for header in headers]


def get_user_events_by_month():
    pass


def get_params():
    save_full = True if "save-full" in sys.argv else False
    save_condensed = True if "save-condensed" in sys.argv else False

    return save_full, save_condensed


def cleanup(line):
    line = line.strip()

    if line[0] == "[":
        line


def read_large_json_by_row(blob_client):
    try:
        storage_stream_downloader = blob_client.download_blob()
    except ResourceNotFoundError:
        raise ResourceNotFoundError("The blob specified was not found.")

    headers = []
    from_previous_chunk = ""

    for chunk_index, chunk in enumerate(storage_stream_downloader.chunks()):
        print(f"Chunk Index: {chunk_index}")

        try:
            chunk_str = chunk.decode("utf-8-sig")
        except Exception as e:
            print("Could not decode chunk", e)
            sys.exit()

        lines = chunk_str.split("\n")

        if from_previous_chunk != "":
            # print("Adding data from previous chunk:")
            # print(from_previous_chunk)
            # print()
            # print(lines[0])
            lines[0] = from_previous_chunk + lines[0]
            from_previous_chunk = ""  # should this be here?

        for line_index, _line in enumerate(lines):
            line = _line  # = cleanup(_line)
            # print("type:", type(line))
            # print("line:", line)
            # print()

            # If last line in the chunk, check if the object is complete
            if line_index == len(lines) - 1:
                # Has matching curly braces
                if line.count("{") == line.count("}"):
                    print("has matching number of curly braces")
                else:
                    print("doesn't have matching number of curly braces")

                if line.endswith("}") and (line.count("{") == line.count("}")):
                    print("all good")
                    from_previous_chunk = ""
                else:
                    print("incomplete")
                    from_previous_chunk = line
                    continue

            # Clean up line variable and convert to json
            try:
                matches = re.findall(REGEX_JSON_PATTERN, line)
                # print("matches:", matches)
                # .search(REGEX_JSON_PATTERN, line)

                # if there are multiple matches, then there is nested {} braces
                # loop over the remaining main matches and replace them with
                # the inner contents
                if len(matches) > 1:
                    # print("Attempting to replace inner json with something else...")

                    other_matches = matches[1:]

                    for index, match in enumerate(other_matches, 1):
                        # contents inside the {} braces
                        new_contents = re.findall(r"{(.*)}", matches[index])[0]

                        old_contents = match.replace('"', "'")

                        # print("\tNew contents:", new_contents)

                        # update line with curly braces removed
                        line = re.sub(old_contents, new_contents, line)

                    matches = re.findall(REGEX_JSON_PATTERN, line)

                if len(matches) == 0:
                    # Empty; no results found
                    print("regex: no matches found")
                else:
                    # regex_result = matches.group(1)

                    regex_result = matches[0]

                    # print("Regex result:", regex_result)

            except Exception as e:
                print("Couldn't perform regex:", e)
                print(f"# of matches: {len(matches)}")
                traceback.print_exc()
                sys.exit()

            try:
                json_row_data = json.loads(regex_result)
            except Exception as e:
                print("=" * 40)
                print("Attempted to use json.loads", end="\n\n")
                print("Error:", e, end="\n\n")
                print("Regex result:", regex_result, end="\n\n")
                print("Line:", line, end="\n\n")
                print(f"Chunk: {chunk_index} Line: {line_index}", end="\n\n")
                traceback.print_exc()
                print("=" * 40)
                sys.exit()

            # print(json_row_data)
            # sys.exit()

            yield json_row_data

            # If first chunk and first line, write headers
            # if chunk_index == 0 and line_index == 0:
            #     headers = sorted(json_row_data.keys())
            #     row_data = headers
            # else:
            #     # Convert json to list
            #     row_data = json_to_ordered_list(json_row_data, headers)

            # full_data_path = os.path.join("data", "full", file_name + ".csv")
            # append_row_to_csv(full_data_path, row_data)


def append_row_to_csv(csv_path, row_data):
    with open(csv_path, "a", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)

        try:
            csv_writer.writerow(row_data)
        except Exception as e:
            print(row_data)
            print(f"Error: {e}")
            sys.exit()


if __name__ == "__main__":
    # Load Environment Variables
    load_dotenv()
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    # Choose a specific date to ensure consistent data pulls
    date_and_time = datetime(year=2022, month=5, day=17)

    # Instantiate blob service client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    except Exception as e:
        print(f"Unable to connect to BlobServiceClient: {e}")

    container_client = blob_service_client.get_container_client(DEFAULT_AZURE_CONTAINER)

    pattern_match = (
        "Unprocessed/Redshift/"
        + str(date_and_time.year)
        + "/"
        + str(date_and_time.month).zfill(2)
        + "/"
        + str(date_and_time.day).zfill(2)
        + "/"
    )
    # pattern_match = r"^" + pattern_match

    blobs = container_client.list_blobs(name_starts_with=pattern_match)

    print(pattern_match)

    file_sizes = {}

    for index, blob in enumerate(blobs):
        table_name = blob.name.split("/")[-1]
        # blob_client = blob_service_client.get_blob_client(
        #     DEFAULT_AZURE_CONTAINER, blob.name
        # )

        # result = re.match(pattern_match, blob.name)

        # print(result)

        # # <class 'azure.storage.blob._models.BlobProperties'>
        # print(blob.name, blob.size)

        # print("\t", blob)

        # if index > 5:
        # if result:
        #     sys.exit()
        file_sizes[table_name] = blob.size

    pprint(file_sizes)
    rankings = sorted(file_sizes.items(), key=lambda x: x[1], reverse=True)
    pprint(rankings[:25])
    print(len(rankings))

    output_path = os.path.join("output", "ranked_events.csv")

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        # create the csv writer
        writer = csv.writer(f)

        for row in rankings:
            # write a row to the csv file
            writer.writerow(row)
    sys.exit()

    # Get users
    users_df = get_users(blob_service_client, date_and_time)
    # Remove unneeded columns
    users_df = users_df.drop(
        ["clientid", "additionalinvitationsleft", "data_source_id", "sync_date"], axis=1
    )
    # Remove non-english and inactive users
    # users_df = users_df[(users_df['isactive'] == True)] # ToDo: add a column designating is_active
    # Remove users if they were created less that 60 days ago
    users_df["datecreated"] = pd.to_datetime(
        users_df["datecreated"], format="%Y-%m-%dT%H:%M:%S", errors="coerce"
    )
    six_months_ago = datetime.datetime.today() - datetime.timedelta(
        days=60
    )  # start of 2021
    users_df = users_df[users_df["datecreated"] < six_months_ago]
    # Add prefixes for table clarity
    users_df = users_df.add_prefix("user_")

    # Get accounts
    accounts_df = get_accounts(blob_service_client, date_and_time)
    # Remove unnecessary columns
    accounts_df = accounts_df.drop(
        ["ownerid", "edition", "planid", "settings", "data_source_id", "sync_date"],
        axis=1,
    )
    # Keep active accounts
    # accounts_df = accounts_df[accounts_df['isactive'] == True]
    # Fix datetime
    accounts_df["datecreated"] = pd.to_datetime(
        accounts_df["datecreated"], format="%Y-%m-%dT%H:%M:%S", errors="coerce"
    )
    # Add prefixes for table clarity
    accounts_df = accounts_df.add_prefix("account_")
    print(users_df)

    event_data = {}
    combined_data = None

    for table_name in event_table_names:
        full_table_name = get_full_blob_name(
            "Unprocessed", "Redshift", table_name, date_and_time
        )
        file_name = table_name.split(".")[0]

        print(file_name)

        blob_client = blob_service_client.get_blob_client(
            "researchanalyticsinsights", full_table_name
        )

        # Blob size in Gigabytes (Gb)
        # Conversion seen here: https://gbmb.org
        blob_size = blob_client.get_blob_properties().size / (2**30)
        print(f"\tBlob size: {blob_size:.2f} Gb")

        if blob_size >= 20:
            # continue
            full_data_path = os.path.join("data", "full", file_name + ".csv")
            data = {}
            headers = []
            first_row = True

            for json_row_data in read_large_json_by_row(blob_client):
                try:
                    # If first chunk and first line, write headers
                    if first_row:
                        # print("\tfirst row")
                        headers = sorted(json_row_data.keys())
                        row_data = headers
                    else:
                        # Convert json to list
                        row_data = json_to_ordered_list(json_row_data, headers)

                    try:
                        append_row_to_csv(full_data_path, row_data)
                    except Exception as e:
                        print("Could not append to file.", e)
                        sys.exit()

                    if first_row:
                        first_row = False
                        continue

                    # Lookup variables
                    user_pos = headers.index("user_id")
                    user = row_data[user_pos]

                    time_stamp = date_parser.parse(row_data[headers.index("timestamp")])
                    event_month = str(time_stamp.month).zfill(2)
                    event_year = str(time_stamp.year)
                    row_key = f"{user}|{event_year}|{event_month}"

                    # Filter out any unwanted rows here

                    # Store info on aggregated table
                    if row_key not in data.keys():
                        data[row_key] = 0

                    data[row_key] += 1

                except Exception as e:
                    print("Something went wrong", e)
                    sys.exit()

            # Convert data into dataframe
            df = pd.DataFrame(data, columns=["user_year_month", f"{file_name}.count"])
            print(df)
            sys.exit()
        else:
            continue
            print(f"Converting {full_table_name} to dataframe...")

            df = get_json_blob_as_df(
                blob_service_client, "researchanalyticsinsights", full_table_name
            )

            # print(event_data[full_table_name].head())

            df["timestamp"] = pd.to_datetime(
                df["timestamp"], infer_datetime_format=True
            )

            df["month"] = df["timestamp"].map(lambda x: str(x.month).zfill(2))
            df["year"] = df["timestamp"].map(lambda x: str(x.year))
            df["year_month"] = df["year"].str.cat(df["month"], sep="-")

            print(df["year_month"].head())

            if save_full:
                df.to_csv(os.path.join("data", "full", file_name + ".csv"), index=False)

            """
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
            """

            num_events_by_group_df = (
                df.groupby(["user_id", "year", "month"])
                .size()
                .to_frame(name=f"{file_name}.count")
            )

            print(num_events_by_group_df)

            if combined_data is None:
                combined_data = num_events_by_group_df
            else:
                print("Join")
                print(combined_data)
                combined_data = combined_data.join(
                    num_events_by_group_df, on=["user_id", "year", "month"]
                )
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

    df = combined_data.merge(
        users_df, how="left", left_on="user_id", right_on="user_id"
    )

    print(df.head())

    # Get number of users per account
    account_sum = accounts_df.merge(
        users_df, how="inner", left_on="account_id", right_on="user_accountid"
    )
    account_sum = (
        account_sum.groupby("account_id")["user_id"]
        .count()
        .reset_index()
        .rename(columns={"user_id": "user_count"})
    )

    print("added number of users per account")

    # Get backend users
    backendusers_df = get_json_blob_as_df(
        blob_service_client,
        "researchanalyticsinsights",
        "Unprocessed/Redshift/2022/02/24/backendusers.json",
    )

    print("get backend schema data")

    # backendusers_df.info()

    # Remove unnecessary columns
    backendusers_df = backendusers_df.drop(
        [
            "received_at",
            "uuid",
            "editionname",
            "account_id",
            "account_name",
            "accountstatus",
            "avatar",
            "context_library_name",
            "company_account_status",
            "uuid_ts",
            "accountcreated",
            "company_id",
            "context_library_version",
            "trialends",
            "company_plan",
            "created_at",
            "editionplanid",
            "context_group_id",
            "deleted",
            "company_status",
            "status",
            "company_edition",
            "experiments",
            "is_primary",
        ],
        axis=1,
    )

    # Add prefix
    backendusers_df = backendusers_df.add_prefix("backenduser_")

    # Join backend with users
    user_df = users_df.merge(
        backendusers_df, how="inner", left_on="user_id", right_on="backenduser_id"
    )

    # Join sum with accounts
    account_df = accounts_df.merge(
        account_sum, how="inner", left_on="account_id", right_on="account_id"
    )

    # Drop additional user columns
    user_df = user_df.drop(
        [
            "backenduser_id",
            "backenduser_company_created_at",
            "backenduser_last_name",
            "backenduser_first_name",
            "user_language",
        ],
        axis=1,
    )
    # Drop additional account columns
    account_df = account_df.drop(["account_language"], axis=1)

    # Join users and accounts
    _df = user_df.merge(
        account_df, how="inner", left_on="user_accountid", right_on="account_id"
    )

    # Drop duplicate column
    _df = df.drop(["user_accountid"], axis=1)

    print("df:")
    print(_df.head())

    # NaN to 0
    # tasks_df = tasks_df.fillna(0)

    # Merge event data with users
    # df = df.merge(tasks_df, how='inner', left_on='user_id', right_on='user_id')

    # Remove duplicate rows
    # df = df[~df.duplicated(keep='last')]

    # Subsription SQL
    # ```
    # SELECT
    #     subscription_id,
    #     subscription_mrr
    # FROM chargebee_rest_subscriptions_all
    # WHERE subscription_id IN (<list-of-subscription-ids-from-df>)
    # ORDER BY subscription_id
    # ```

    print("Read in expansion data...")
    subscriptions = pd.read_csv("chargebee_subscriptions.csv")

    print(subscriptions.head())

    # Merge subscriptions with users
    # df = df.merge(subscriptions, how='inner', left_on='account_subscriptionid', right_on='subscription_id')

    # Remove $0 subscriptions
    # df = df[df.subscription_mrr > 0]

    # Remove gtmhub & primeholding users
    # df = df[~df.backenduser_email.str.contains('primeholding')]
    # df = df[~df.backenduser_email.str.contains('gtmhub')]

    # Remove unnecessary columns
    # df = df.drop(['account_subscriptionid', 'subscription_id', 'user_id'], axis=1)

    # Write user sample to CSV
    # df.to_csv(
    #     os.path.join(
    #         "data",
    #         "condensed",
    #         "event_data.csv"
    #     ),
    #     index=False
    # )
