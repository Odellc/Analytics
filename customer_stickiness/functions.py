import os
import sys
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from dotenv import load_dotenv
import pandas as pd
import datetime
from dateutil import parser as date_parser
import json
from pprint import pprint


def get_invalid_json(blob_client, container, blob_path):
    """
    Get a JSON blob from Azure and read it into a pandas dataframe.
    Params:
        :blob_client (BlobServiceClient object): Azure blob service client object
        :container (str): Name of the Azure storage container
        :blob_path (str): Name of the Azure blob
    """
    blob_client_instance = blob_client.get_blob_client(container, blob_path)
    streamdownloader = blob_client_instance.download_blob()

    byte_contents = streamdownloader.readall()
    str_contents = byte_contents.decode("utf-8-sig")
    split_contents = str_contents.split("\r\n")

    contents = [
        json.loads(content)
        for content in split_contents
        if content != ""
    ]
    
    return contents


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


def json_csv(data):
    new_data = []

    def inner(piece, row, column_parts):
        data_type = type(piece)

        if data_type == list:
            for item in piece:
                inner(item, row, column_parts)
                row += 1
        elif data_type == dict:
            # Print all keys
            # print("row:", row)
            for key in piece.keys():
                # print("  ", key)
                column_name = ".".join(column_parts)
                inner(piece[key], row, column_parts + [key])
        else:
            # Primitive data type
            max_index = len(new_data) - 1

            if row > max_index:
                new_data.append({})

            # print(max_index, row)
            new_data[row][".".join(column_parts)] = piece

    inner(data, 0, [])

    # pprint(new_data[0:5])

    return new_data


def datetime_string(date_time, delimeter):
    year = date_time.year
    month = str(date_time.month).zfill(2)
    day = str(date_time.day).zfill(2)

    return f"{year}{delimeter}{month}{delimeter}{day}"


def epoch_to_datetime(epoch):
    # There was a negative value in users.json
    # This might be a problem elsewhere.
    timestamp = abs(epoch)

    try:
        return datetime.datetime.fromtimestamp(0) + datetime.timedelta(milliseconds=timestamp)
    except Exception as e:
        print(timestamp)
        print(e)
        
        # sys.exit()
        return 42


def export_tag_frequency(blob_service_client, today_str):
    # Get MongoDB tag information
    print("Fetching MongoDB tags data...")

    table_name = "tags"

    data = []

    for continent_code in ("EU", "US"):
        tags_json = get_invalid_json(
            blob_service_client, 
            "researchanalyticsinsights", 
            f"Unprocessed/Gtmhub MongoDB {continent_code}/{today_str}/{table_name}.json"
        )

        data.append(tags_json)

    tags_df = pd.DataFrame(json_csv(data))

    tags_df = tags_df.rename(
        columns={
            "_id.$oid": "user_id",
            "accountId.$oid": "account_id",
            "dateCreated.$date": "date_created",
            "itemsTaggedCount": "items_tagged_count",
            "lastUsed.$date": "last_used"
        }
    )

    # Find the frequency of each tag
    tags_freq = {}

    for row_data in tags_df.itertuples():
        tag_name = row_data.name
        tag_count = row_data.items_tagged_count

        if tag_name not in tags_freq.keys():
            tags_freq[tag_name] = [tag_name, 0]

        tags_freq[tag_name][1] += tag_count

    tags_freq_df = pd.DataFrame(
        [value for value in tags_freq.values()],
        columns=["tag_name", "tag_count"]
    )

    output_path = os.path.join("output", f"{table_name}_frequency.csv")
    print(f"Exporting {output_path}...")

    tags_freq_df.to_csv(output_path, index=False)


def get_hubspot_data():
    print("Fetching Hubspot data...")

    shared_data_path = os.path.join(os.pardir, "shared_data")

    # Hubspot sources
    contacts_path = os.path.join(shared_data_path, "hubspot_contacts.json")
    companies_path = os.path.join(shared_data_path, "hubspot_companies.json")

    print("Hubspot contacts...")
    # Get HubSpot Contacts
    hs_contacts = pd.read_json(contacts_path)

    # print("before")
    # print(hs_contacts.head())
    # pprint(list(hs_contacts.columns))

    # Explode properties
    hs_contacts = hs_contacts.join(hs_contacts.properties.apply(pd.Series))

    # print("after")
    # print(hs_contacts.head())
    # pprint(list(hs_contacts.columns))

    # print("Hubspot Contacts columns:")
    # print(list(hs_contacts.columns))

    # Keep necessary columns
    hs_contacts = hs_contacts[['associatedcompanyid', 'email', 'hs_object_id', 'jobtitle']]

    # Get HubSpot Companies
    print("Hubspot companies...")
    hs_companies = pd.read_json(companies_path)

    # Explode properties
    hs_companies = hs_companies.join(hs_companies.properties.apply(pd.Series))

    # print(hs_companies.head())
    # print("Hubspot Companies columns:")
    # print(list(hs_companies.columns))

    # Keep necessary columns
    hs_companies = hs_companies[
        ['hs_object_id', 'industry', 'numberofemployees', 'chargebee_id', 'country']
    ]

    # Merge hubspot contacts with companies 
    hubspot = hs_contacts.merge(
        hs_companies, 
        how='left', 
        left_on='associatedcompanyid', 
        right_on='hs_object_id'
    )
    
    hubspot = hubspot.drop(['hs_object_id_x', 'hs_object_id_y'], axis=1)

    hubspot = hubspot.add_prefix("hubspot|")

    # pprint(list(hubspot.columns))

    return hubspot


def get_accounts_data(blob_service_client, today_str):
    # Get accounts
    print("Fetching account data...")
    eu_accounts = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub MongoDB EU/{today_str}/accounts.json"
    )
    us_accounts = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub MongoDB US/{today_str}/accounts.json"
    )
    accounts_df = pd.DataFrame(json_csv(eu_accounts + us_accounts))

    # Drop unneeded columns / keep only specified columns
    accounts_df = accounts_df[
        accounts_df.columns[
            accounts_df.columns.isin(
                [
                    '_id.$oid',
                    'name',
                    'domain',
                    'isActive',
                    'type',
                    'trialEnds.$date',
                    'dateCreated.$date',
                    'ownerId.$oid',
                    'edition',
                    'subscriptionId',
                    'planId',
                    'subscription.mrr',
                    'billingSystem',
                    'language',
                    'modifiedById.$oid',
                    'modifiedAt.$date'
                ]
            )
        ]
    ]

    # accounts_df["dateCreated.$date"] = accounts_df["dateCreated.$date"].map(epoch_to_datetime)
    accounts_df["dateCreated.$date"] = pd.to_datetime(
        accounts_df["dateCreated.$date"],
        unit="ms"
    )

    return accounts_df


def get_chargebee_subscriptions(blob_service_client, today_str):
    """
    Fetch and minimally process Chargebee subscription data
    """
    print("Fetching Chargebee data...")

    eu_chargebee = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Chargebee EU/{today_str}/subscriptions.json"
    )
    us_chargebee = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Chargebee US/{today_str}/subscriptions.json"
    )

    df = pd.DataFrame(json_csv(eu_chargebee + us_chargebee))

    # print(chargebee_df.head())
    # print(chargebee_df.columns)

    # print(chargebee_df["customer_id"].head())

    # Drop unneeded columns / keep only specified columns
    df = df[
        df.columns[
            df.columns.isin(
                [
                    'id',
                    'currency_code',
                    'trial_end',
                    'customer_id',
                    'status',
                    'trial_start',
                    'created_at',
                    'started_at',
                    'cancelled_at',
                    'cancel_reason',
                    'updated_at',
                    'mrr',
                    'deleted',
                    'activated_at',
                    'shipping_address.email',
                    'shipping_address.phone',
                    'shipping_address.line1',
                    'shipping_address.city',
                    'shipping_address.country',
                    'shipping_address.first_name',
                    'shipping_address.last_name',
                    'invoice_notes',
                    'exchange_rate',
                    'base_currency_code',
                    'shipping_address.company'
                ]
            )
        ]
    ]

    return df


def get_users_data(blob_service_client, today_str):
    # Get users
    print("Fetching user data...")

    eu_users = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub MongoDB EU/{today_str}/users.json"
    )
    us_users = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Gtmhub MongoDB US/{today_str}/users.json"
    )
    users_df = pd.DataFrame(json_csv(eu_users + us_users))

    # Drop unneeded columns / keep only specified columns
    users_df = users_df[
        users_df.columns[
            users_df.columns.isin(
                [
                    '_id.$oid',
                    'accountId.$oid',
                    'language',
                    'dateCreated.$date',
                    'auth0Cache.email',
                    'auth0Cache.usermetadata.lastName',
                    'auth0Cache.usermetadata.firstName',
                    'subscriptionType',
                    'isPrimary',
                    'auth0Cache.usermetadata.demo'
                ]
            )
        ]
    ]

    users_df = users_df.rename(columns={"auth0Cache.email": "email"})

    users_df["email"] = users_df["email"].fillna("-")

    users_df = users_df[
        ~users_df["email"].str.contains("gtmhub") &
        ~users_df["email"].str.contains("primeholding")
    ]

    print(users_df.info())

    # Handle the weird negative timestamp.
    # users_df["dateCreated.$date"] = users_df["dateCreated.$date"].map(lambda dt: abs(dt))

    # users_df[["dateCreated.$date"]].to_csv("users_datecreated.csv")
    

    # Convert user date created field from epoch(ms) -> datetime
    users_df["dateCreated.$date"] = users_df["dateCreated.$date"].map(epoch_to_datetime)
    # users_df["dateCreated.$date"] = users_df["dateCreated.$date"].astype("datetime64[ns]")

    # users_df["dateCreated.$date"] = pd.to_datetime(
    #     users_df["dateCreated.$date"],
    #     unit="ms"
    # )

    return users_df


def get_users_accounts_subscriptions_data(blob_service_client, today_str):
    users_df = get_users_data(blob_service_client, today_str)    

    # Add prefixes for table clarity
    users_df = users_df.add_prefix('user|')

    accounts_df = get_accounts_data(blob_service_client, today_str)

    # Add prefixes for table clarity
    accounts_df = accounts_df.add_prefix('account|')

    chargebee_df = get_chargebee_subscriptions(blob_service_client, today_str)

    # Add prefixes for table clarity
    chargebee_df = chargebee_df.add_prefix('chargebee|')

    # Join accounts and chargebee
    accounts_df = accounts_df.merge(
        chargebee_df, 
        how='inner', 
        left_on='account|_id.$oid', 
        right_on='chargebee|customer_id'
    )

    #========================================
    # Apply filters to accounts
    #========================================
    # Keep active accounts
    accounts_df = accounts_df[accounts_df['account|isActive'] != False]

    # Keep paid accounts with MRR > 0
    accounts_df = accounts_df[accounts_df["chargebee|mrr"] > 0]
    
    print(f"# of all active, paid accounts: {accounts_df.shape}")

    # Merge accounts and users tables so that we only keep users who belong
    # to the accounts table
    print("Merging user and account table...")
    df = accounts_df.merge(
        users_df,
        how="inner",
        left_on="account|_id.$oid",
        right_on="user|accountId.$oid"
    )

    print(f"Rows of users after merge: {df.shape}")

    # Remove duplicate rows
    df = df[~df.duplicated(keep='last')]
    print(f"Rows in users_df: {df.shape}")

    # Add number of users per account as a field
    users_per_account = df.groupby("account|_id.$oid")["user|_id.$oid"].count().reset_index().rename(
        columns={
            "user|_id.$oid": "account|users_count",
            "account|_id.$oid": "account|id"
        }
    )

    print("Rows in user per account", users_per_account.shape)

    df = df.merge(
        users_per_account,
        how="left",
        left_on="account|_id.$oid",
        right_on="account|id"
    )

    print("Size of all users of active, paid accounts", df.shape)

    return df


def get_task_created(blob_service_client, today_str):
    tasks_created = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Redshift/{today_str}/backendtask_created.json"
    )
    
    tasks_created = pd.DataFrame(json_csv(tasks_created))

    pprint(list(tasks_created.columns))

    # Drop unneeded columns / keep only specified columns
    # users_df = users_df[
    #     users_df.columns[
    #         users_df.columns.isin(
    #             [
    #                 '_id.$oid',
    #                 'accountId.$oid',
    #                 'language',
    #                 'dateCreated.$date',
    #                 'auth0Cache.email',
    #                 'auth0Cache.usermetadata.lastName',
    #                 'auth0Cache.usermetadata.firstName',
    #                 'subscriptionType',
    #                 'isPrimary',
    #                 'auth0Cache.usermetadata.demo'
    #             ]
    #         )
    #     ]
    # ]

    # users_df = users_df.rename(columns={"auth0Cache.email": "email"})

    # users_df["email"] = users_df["email"].fillna("-")

    # users_df = users_df[
    #     ~users_df["email"].str.contains("gtmhub") &
    #     ~users_df["email"].str.contains("primeholding")
    # ]

    return tasks_created


def get_task_modified(blob_service_client, today_str):
    tasks_modified = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Redshift/{today_str}/backendtask_modified.json"
    )
    
    tasks_modified = pd.DataFrame(json_csv(tasks_modified))

    pprint(list(tasks_modified.columns))

    # Drop unneeded columns / keep only specified columns
    # users_df = users_df[
    #     users_df.columns[
    #         users_df.columns.isin(
    #             [
    #                 '_id.$oid',
    #                 'accountId.$oid',
    #                 'language',
    #                 'dateCreated.$date',
    #                 'auth0Cache.email',
    #                 'auth0Cache.usermetadata.lastName',
    #                 'auth0Cache.usermetadata.firstName',
    #                 'subscriptionType',
    #                 'isPrimary',
    #                 'auth0Cache.usermetadata.demo'
    #             ]
    #         )
    #     ]
    # ]

    # users_df = users_df.rename(columns={"auth0Cache.email": "email"})

    # users_df["email"] = users_df["email"].fillna("-")

    # users_df = users_df[
    #     ~users_df["email"].str.contains("gtmhub") &
    #     ~users_df["email"].str.contains("primeholding")
    # ]

    return tasks_modified


def get_task_deleted(blob_service_client, today_str):
    tasks_deleted = get_invalid_json(
        blob_service_client, 
        "researchanalyticsinsights", 
        f"Unprocessed/Redshift/{today_str}/backendtask_deleted.json"
    )
    
    tasks_deleted = pd.DataFrame(json_csv(tasks_deleted))

    pprint(list(tasks_deleted.columns))

    # Drop unneeded columns / keep only specified columns
    # users_df = users_df[
    #     users_df.columns[
    #         users_df.columns.isin(
    #             [
    #                 '_id.$oid',
    #                 'accountId.$oid',
    #                 'language',
    #                 'dateCreated.$date',
    #                 'auth0Cache.email',
    #                 'auth0Cache.usermetadata.lastName',
    #                 'auth0Cache.usermetadata.firstName',
    #                 'subscriptionType',
    #                 'isPrimary',
    #                 'auth0Cache.usermetadata.demo'
    #             ]
    #         )
    #     ]
    # ]

    # users_df = users_df.rename(columns={"auth0Cache.email": "email"})

    # users_df["email"] = users_df["email"].fillna("-")

    # users_df = users_df[
    #     ~users_df["email"].str.contains("gtmhub") &
    #     ~users_df["email"].str.contains("primeholding")
    # ]

    return tasks_deleted    