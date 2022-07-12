import os
import sys
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from dotenv import load_dotenv
import pandas as pd
import datetime
from dateutil import parser as date_parser
import json
from pprint import pprint
from functions import (
    get_invalid_json,
    get_json_blob_as_df,
    json_csv,
    datetime_string,
    epoch_to_datetime,
    export_tag_frequency,
    get_hubspot_data,
    get_accounts_data,
    get_chargebee_subscriptions,
    get_users_data,
    get_users_accounts_subscriptions_data
)


def main():
    # Environment vars
    load_dotenv()
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Hardcoded date to ensure duplicatable results
    today = datetime.datetime(year=2022, month=4, day=4)
    today_str = datetime_string(today, "/")

    # Instantiate blob service client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    except Exception as e:
        print(f'Unable to connect to BlobServiceClient: {e}')

    df = get_users_accounts_subscriptions_data(blob_service_client, today_str)

    # Convert epoch time to datetime
    df["user|dateCreated.$date"] = df["user|dateCreated.$date"].map(epoch_to_datetime)

    # Keep only users who have had accounts for at least 180 days
    one_hundred_eighty_days_ago = today - datetime.timedelta(days=180)
    df = df[df["user|dateCreated.$date"] <= one_hundred_eighty_days_ago]

    print("Rows of users after 180 days: ", df.shape)

    # Get Amplitude tag information
    source_name = "Redshift"
    schema = "backend"
    _table = "an_objective_tagged_through_bulk_tagging"
    table_name = f"{schema}{_table}"

    amplitude_tags = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/{source_name}/{today_str}/{table_name}.json"
    )

    amplitude_tags = pd.DataFrame(amplitude_tags)

    print("# of rows for events:", amplitude_tags.shape)

    print("amplitude col:", amplitude_tags.columns)

    users_that_triggered_tag = amplitude_tags["user_id"].unique()
    print(
        "[Verification]: Unique # of users in amplitude events:", 
        len(users_that_triggered_tag)
    )
    
    amplitude_tags = amplitude_tags.groupby("user_id")["id"].count().reset_index().rename(
        columns={'id': 'tags_count'}
    )

    amplitude_tags = amplitude_tags.add_prefix('tag_event|')

    print(amplitude_tags.head())
    print(amplitude_tags.columns)
    print("Amplitude rows:", amplitude_tags.shape[0])

    print("unique users in events df:", len(amplitude_tags["tag_event|user_id"].unique()))

    # Verify that there are users that exist in Amplitude events that aren't
    # a user.
    uncaptured_users = 0
    for user in users_that_triggered_tag:
        if user not in df["user|_id.$oid"].unique():
            uncaptured_users += 1

    print("Uncaptured users:", uncaptured_users)

    # Join user, account, chargebee df with the tag df (Amplitude events)
    df = df.merge(
        amplitude_tags,
        how="inner",
        left_on="user|_id.$oid",
        right_on="tag_event|user_id"
    )

    print("Rows of users who threw a tag event: ", df.shape[0])

    # print(df.head())
    # pprint(list(df.columns))

    print("Reading in csda data...")
    csda_df = pd.read_csv(os.path.join(os.pardir, "shared_data", "csda_salesforce_export.csv"))
    csda_df.columns = [
        "_".join(column.lower().split())
        for column in csda_df.columns
    ]

    csda_df = csda_df.add_prefix("salesforce|")

    # df["salesforce|is_csda"] = 

    print("Left join df with csda...")
    df = df.merge(
        csda_df,
        how="left",
        left_on="account|_id.$oid",
        right_on="salesforce|chargebee_id"
    )

    # print(csda_df.head())
    print("New df dim:", df.shape)

    # sys.exit()

    hubspot_df = get_hubspot_data()

    print("New df dim:", df.shape)

    print("Hubspot dim:", hubspot_df.shape)

    print("Left join df with hubspot...")
    df = df.merge(
        hubspot_df,
        how="left",
        left_on="user|auth0Cache.email",
        right_on="hubspot|email"
    )

    # Add CSDA column
    df["salesforce|team_member_name"] = df["salesforce|team_member_name"].fillna("-")
    df["salesforce|is_csda"] = df["salesforce|team_member_name"].map(
        lambda x: True if x != "-" else False
    )

    column_name_remapping = {
        "account|_id.$oid": "account.id",
        "account|name": "account.name",
        "account|dateCreated.$date": "account.date_created",
        "chargebee|status": "chargebee.status",
        "user|_id.$oid": "user.id",
        "user|language": "user.language",
        "user|dateCreated.$date": "user.date_created",
        "user|auth0Cache.email": "user.email",
        "user|auth0Cache.usermetadata.lastName": "user.last_name",
        "user|auth0Cache.usermetadata.firstName": "user.first_name",
        "account|users_count": "account.users_count",
        "tag_event|tags_count": "event.user_tags_count",
        "hubspot|jobtitle": "hubspot.job_title",
        "hubspot|industry": "hubspot.industry",
        "hubspot|numberofemployees": "hubspot.number_of_employees",
        "salesforce|is_csda": "salesforce.is_csda",
        "hubspot|country": "hubspot.country"
    }

    df = df[column_name_remapping.keys()]

    df = df.rename(columns=column_name_remapping)

    print("df dim after merge:", df.shape)

    # Change language code to categorical
    language_code_mapping = {
        0: "English",
        1: "German",
        2: "Chinese",
        3: "Bulgarian",
        4: "Spanish",
        5: "French",
        6: "Portuguese"
    }

    df = df.replace({"user.language": language_code_mapping})

    # Get the number of events by account
    events_per_account = df.groupby("account.id")["event.user_tags_count"].count().reset_index().rename(
        columns={
            "event.user_tags_count": "event.account_tags_count"
        }
    )

    df.merge(
        events_per_account,
        how="left",
        left_on="account.id",
        right_on="account.id"
    )

    # Sort to have companies grouped and ordered
    df = df.sort_values(
        ["event.account_tags_count", "account.id", "event.user_tags_count", "user.id"],
        ascending=[False, False, False, False]
    )

    output_path = os.path.join("output", "tags_data.csv")
    df.to_csv(output_path, index=False)

    # export_tag_frequency(blob_service_client, today_str)


if __name__ == "__main__":
    main()