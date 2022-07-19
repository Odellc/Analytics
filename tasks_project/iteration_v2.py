#!/usr/bin/env python
# coding: utf-8

# # Task Functionality User Sample
# 
# In order to help the research team find Gtmhub users to interview who use the task functionality, the data science and analytics team gathered, transformed, and extracted data on users that met their research criteria. More on the research plan [here](https://dovetailapp.com/projects/2IUhqbkGJ73oTG1mfsTIRq/readme).
# 
# Steps:
# 1. Get User and Account Data from Azure Data Lake.
#     - This data comes from the Gtmhub Raw set of data in Gtmhub. 
#     - There are two sets of data, one from EU and one from US, so we combine them.
# 2. Clean up and filter the user and account data.
#     - Remove unnecessary fields.
#     - Filter users for english speakers, active, and created greater than 6 months ago.
#     - Filter accounts for only active accounts.
# 3. Get Backend Users from Redshift backend schema (in Azure Data Lake).
#     - These users contain additional information that the raw set of users do not contain (e.g., email, name, etc.).
# 4. Clean up Backend Users and merge with Gtmhub Raw users.
# 5. Join user and account data.
# 6. Get task related data from Azure Data Lake (Redshift backend schema).
#     - Three different event tables: task_created, task_modified, task_deleted.
# 7. Group and combine task related data by user.
# 8. Join task data with user data.
# 9. Get HubSpot contacts and companies.
#     - Contacts come from a separate script, `contacts.py` in the hubspot_tap repository.
#     - Companies come from a separate script, `companies.py` in the hubspot_tap repository.
# 10. Clean up and combine the HubSpot contacts and companies.
# 11. Get Chargebee subscriptions.
#     - This data comes from the chargebee_rest_subscriptions_all table from data sources in Gtmhub insights.
#     - SQL query is below.
# 12. Join subscription data with user data.
# 13. Join Hubspot data with user data.
# 14. Output file to csv for delivery to research team.

# In[1]:


# Imports
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from dotenv import load_dotenv
import pandas as pd
import datetime
import json
import os
import sys


def get_json_blob_as_df(blob_client, container, blob_path):
    """
    Get a JSON blob from Azure and read it into a pandas dataframe.
    Params:
        :blob_client (BlobServiceClient object): Azure blob service client object
        :container (str): Name of the Azure storage container
        :blob_path (str): Name of the Azure blob
    """
    blob_client_instance = blob_client.get_blob_client(container, blob_path)
    streamdownloader = blob_client_instance.download_blob()
    file_reader = json.loads(streamdownloader.readall())
    df = pd.DataFrame(file_reader)
    return df


if __name__ == "__main__":
    # Environment vars
    load_dotenv()
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    # Instantiate blob service client
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    except Exception as e:
        print(f'Unable to connect to BlobServiceClient: {e}')

    # Get users and accounts
    eu_users = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Gtmhub/2022/02/24/gtmhubrawuserseu.json")
    us_users = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Gtmhub/2022/02/24/gtmhubrawusersus.json")
    eu_accounts = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Gtmhub/2022/02/24/gtmhubrawaccountseu.json")
    us_accounts = get_json_blob_as_df(blob_service_client, "researchanalyticsinsights", "Unprocessed/Gtmhub/2022/02/24/gtmhubrawaccountsus.json")

    # Combine user & account dfs
    users_df = pd.concat([eu_users, us_users])
    accounts_df = pd.concat([eu_accounts, us_accounts])

    print("user and account df combined")

    # Remove unneeded columns
    users_df = users_df.drop(['clientid', 'additionalinvitationsleft', 'data_source_id', 'sync_date'], axis=1)
    # Remove non-english and inactive users
    users_df = users_df[(users_df['language'] == 'english') & (users_df['isactive'] == True)]
    # Remove users if they were created less that 60 days ago
    users_df['datecreated'] = pd.to_datetime(users_df['datecreated'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')
    six_months_ago = datetime.datetime.today() - datetime.timedelta(days=60)
    users_df = users_df[users_df['datecreated'] < six_months_ago]

    print("filter out users with < 60 day usage")

    # Remove unnecessary columns
    accounts_df = accounts_df.drop(['type', 'trialends', 'ownerid', 'edition', 'planid', 'hasslackintegration', 'settings', 'data_source_id', 'sync_date'], axis=1)
    # Keep active accounts
    accounts_df = accounts_df[accounts_df['isactive'] == True]
    # Fix datetime
    accounts_df['datecreated'] = pd.to_datetime(accounts_df['datecreated'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')

    # Add prefixes for table clarity
    users_df = users_df.add_prefix('user_')
    accounts_df = accounts_df.add_prefix('account_')

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
    print("\tdone")

    # sys.exit()

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
    df.to_csv('task_user_sample_2022-03-01.csv', index=False)

    print("finished")
    sys.exit()