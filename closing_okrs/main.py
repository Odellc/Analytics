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
    get_users_accounts_subscriptions_data,
    get_task_created,
    get_task_modified,
    get_task_deleted
)


def main():
    # Environment vars
    load_dotenv()
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Hardcoded date
    # This is to ensure duplicatable results and to have it sync
    # with the hardcoded Gtmhub insightboard number representing
    # Total users from paying Gtmhub accounts (filtered by MRR)
    today = datetime.datetime(year=2022, month=4, day=4)
    today_str = datetime_string(today, "/")

    # Taken @ 2022-04-04 12:54PM PST
    total_paying_users = 44024

    # Instantiate blob service client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    except Exception as e:
        print(f'Unable to connect to BlobServiceClient: {e}')

    # Fetching users, accounts, and chargebee subscriptions.
    # Merging all tables into one.
    df = get_users_accounts_subscriptions_data(blob_service_client, today_str)

    print("Total users eligble to login from paid accounts: ", df.shape[0])

    # Get tasks data
    print("Fetching tasks data...")
    print("=" * 40)
    tasks_data_eu = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub MongoDB EU/{today_str}/tasks.json"
    )
    tasks_data_us = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub MongoDB US/{today_str}/tasks.json"
    )
    tasks_df = pd.DataFrame(json_csv(tasks_data_eu + tasks_data_us))
    tasks_df = tasks_df.add_prefix('task|')

    print(tasks_df.head())
    pprint(tasks_df.columns.tolist())

    print("Redshift Events")
    print("=" * 40)

    print("df dim:")
    print(df.shape)
    print(len(df["user|_id.$oid"].unique()))

    # Last 60 days
    threshold_date = today - datetime.timedelta(days=60)

    print("Task Created")
    tasks_created = get_task_created(blob_service_client, today_str)

    print("Task Modified")
    tasks_modified = get_task_modified(blob_service_client, today_str)

    print("Task Deleted")
    tasks_deleted = get_task_deleted(blob_service_client, today_str)

    print(tasks_created["timestamp"].head())

    print("dim before:", df.shape)

    # Convert "timestamp" from str -> datetime
    tasks_created["timestamp"] = tasks_created["timestamp"].map(date_parser.parse)
    tasks_modified["timestamp"] = tasks_modified["timestamp"].map(date_parser.parse)
    tasks_deleted["timestamp"] = tasks_deleted["timestamp"].map(date_parser.parse)

    # Filter on last 60 days
    tasks_created = tasks_created[tasks_created["timestamp"] >= threshold_date]
    tasks_modified = tasks_modified[tasks_modified["timestamp"] >= threshold_date]
    tasks_deleted = tasks_deleted[tasks_deleted["timestamp"] >= threshold_date]

    # Group on user's id to find count of each event by user
    tasks_created_per_user = tasks_created.groupby("user_id")["id"].count().reset_index().rename(
        columns={"id": "task_created_count"}
    )
    print(tasks_created_per_user.head())
    print(tasks_created_per_user.shape)
    print(len(tasks_created_per_user["user_id"].unique()))
    print()

    tasks_modified_per_user = tasks_modified.groupby("user_id")["id"].count().reset_index().rename(
        columns={"id": "task_modified_count"}
    )
    print(tasks_modified_per_user.head())
    print(tasks_modified_per_user.shape)
    print(len(tasks_modified_per_user["user_id"].unique()))
    print()

    tasks_deleted_per_user = tasks_deleted.groupby("user_id")["id"].count().reset_index().rename(
        columns={"id": "task_deleted_count"}
    )
    print(tasks_deleted_per_user.head())
    print(tasks_deleted_per_user.shape)
    print(len(tasks_deleted_per_user["user_id"].unique()))
    print()

    # Left join on original df
    df = df.merge(
        tasks_created_per_user,
        how="left",
        left_on="user|_id.$oid",
        right_on="user_id"
    )
    print("dim after:", df.shape)

    df = df.merge(
        tasks_modified_per_user,
        how="left",
        left_on="user|_id.$oid",
        right_on="user_id"
    )
    print("dim after:", df.shape)

    df = df.merge(
        tasks_deleted_per_user,
        how="left",
        left_on="user|_id.$oid",
        right_on="user_id"
    )

    print("dim after:", df.shape)

    print("Reading in csda data...")
    csda_df = pd.read_csv(os.path.join(os.pardir, "shared_data", "csda_salesforce_export.csv"))
    csda_df.columns = [
        "_".join(column.lower().split())
        for column in csda_df.columns
    ]

    csda_df = csda_df.add_prefix("salesforce|")

    print("Left join df with csda...")
    df = df.merge(
        csda_df,
        how="left",
        left_on="account|_id.$oid",
        right_on="salesforce|chargebee_id"
    )

    print("*" * 40)
    pprint(df.columns.tolist())
    print("*" * 40)
    # sys.exit()

    hubspot_df = get_hubspot_data()

    print(hubspot_df.head())
    print(hubspot_df.columns)

    # Merge users with HubSpot data
    print("Left join df with hubspot...")
    df = df.merge(
        hubspot_df,
        how="left",
        left_on="user|email",
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
        "user|email": "user.email",
        "user|auth0Cache.usermetadata.lastName": "user.last_name",
        "user|auth0Cache.usermetadata.firstName": "user.first_name",
        "account|users_count": "account.users_count",
        "task_created_count": "event.task_created_count",
        "task_modified_count": "event.task_modified_count",
        "task_deleted_count": "event.task_deleted_count",
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

    # Write user sample to CSV
    df.to_csv(f"task_usage_data_{datetime_string(today, '-')}.csv", index=False)

    sys.exit()

    # Remove unneeded columns
    # users_df = users_df.drop(['clientid', 'additionalinvitationsleft', 'data_source_id', 'sync_date'], axis=1)

    # Remove non-english and inactive users
    # users_df = users_df[(users_df['language'] == 'english') & (users_df['isactive'] == True)]

    # Remove unnecessary columns
    # accounts_df = accounts_df.drop(['type', 'trialends', 'ownerid', 'edition', 'planid', 'hasslackintegration', 'settings', 'data_source_id', 'sync_date'], axis=1)
    
    

    # Fix datetime
    # accounts_df['datecreated'] = pd.to_datetime(accounts_df['datecreated'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')

    # Filter out gtmhub and primeholding accounts

    # For each user:
    # see if they have any associated tasks
    # see if they have interacted with a task at some point in last 60 days
    # for row in 

    # Get number of users per account
    account_sum = accounts_df.merge(users_df, how='inner', left_on='account_id', right_on='user_accountid')
    account_sum = account_sum.groupby('account_id')['user_id'].count().reset_index().rename(columns={'user_id': 'user_count'})

    print("added number of users per account")

    # Get backend users
    backendusers_df = get_invalid_json(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendusers.json")

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
    task_created = get_invalid_json(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendtask_created.json")
    task_deleted = get_invalid_json(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendtask_deleted.json")
    task_modified = get_invalid_json(blob_service_client, "researchanalyticsinsights", "Unprocessed/Redshift/2022/02/24/backendtask_modified.json")

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
    print("\tdone")

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
    df.to_csv(f"task_usage_data_{datetime_string(today, '-')}.csv", index=False)

if __name__ == "__main__":
    main()