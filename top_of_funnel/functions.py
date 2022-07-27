import os
import sys
import pandas as pd
import json
from dateutil import parser as datetime_parser
from datetime import datetime
from pprint import pprint
from glob import glob


def newest_file_in_dir(dir_path, file_extension="csv"):
    """Not yet complete"""
    list_of_files = glob(dir_path + f"*.{file_extension}")
    latest_file = max(list_of_files, key=os.path.getctime)

    return latest_file


def datetime_string(date_time, delimeter):
    year = date_time.year
    month = str(date_time.month).zfill(2)
    day = str(date_time.day).zfill(2)

    return f"{year}{delimeter}{month}{delimeter}{day}"


def get_salesforce_lookup(row):
    print(row)
    print(row.name)

    contact_id = row["salesforcecontactid"]
    lead_id = row["salesforceleadid"]

    return contact_id if contact_id != "" else lead_id


def combine_files_in_dir(dir_path):
    all_data = []
    all_headers = []

    for file_name in os.listdir(dir_path):
        # print(file_name)

        # Read file
        path = os.path.join(dir_path, file_name)

        data = []
        headers = []

        df = pd.read_excel(path)

        all_data.append(df)

        for col_name in df.columns:
            if col_name not in all_headers:
                # print(f"\t{col_name} was not in all headers")
                all_headers.append(col_name)

    df = pd.concat(all_data)

    return df


def get_hubspot_contact_campaign_data():
    # Hubspot contact + campaign data
    hs_contacts = pd.read_csv(os.path.join("data", "hubspot_contacts.csv"))

    # Take each campaign__paid in propertiesWithHistory and create a new row based on it
    hs_contact_campaign = []
    # count = 0

    for index, row in hs_contacts.iterrows():
        # print(row)

        properties_with_history = row["propertiesWithHistory"].replace("'", '"')

        history = json.loads(properties_with_history)

        for campaign_paid, campaign_source in zip(
            history["campaign___paid"], history["campaign_source"]
        ):
            # print(item)
            existing_row_data = {
                header: row[header]
                for header in row.index
                if "properties" not in header
            }

            # Add prefixes to avoid naming collusions
            paid = {
                f"campaign_paid.{key}": value for key, value in campaign_paid.items()
            }
            source = {
                f"campaign_source.{key}": value
                for key, value in campaign_source.items()
            }

            hs_contact_campaign.append({**existing_row_data, **paid, **source})

    hs_contact_campaign_df = pd.DataFrame(hs_contact_campaign)

    # Reference the two columns containing Salesforce IDs
    contact_ids = hs_contact_campaign_df["salesforcecontactid"]
    lead_ids = hs_contact_campaign_df["salesforceleadid"]

    # Create a new column to handle the Salesforce join
    hs_contact_campaign_df["salesforce_id"] = contact_ids.combine_first(lead_ids)

    # Convert timestamp to datetime
    hs_contact_campaign_df["campaign.created_at"] = hs_contact_campaign_df[
        "campaign_paid.timestamp"
    ].map(datetime_parser.parse)

    # Filter down data to 2020 and later
    hs_contact_campaign_df = hs_contact_campaign_df[
        hs_contact_campaign_df["campaign.created_at"]
        >= datetime(year=2020, month=1, day=1).isoformat()
    ]

    hs_contact_campaign_df = hs_contact_campaign_df.sort_values(
        ["id", "campaign.created_at"]
    )
    # .drop_duplicates(subset="id", keep="first", ignore_index=True)

    # Drop the unnecessary columns
    hs_contact_campaign_df.drop(
        columns=[
            "createdAt",
            "updatedAt",
            "archived",
            "campaign___paid",
            "campaign_source",
            "createdate",
            "hs_analytics_source",
            "hs_analytics_source_data_1",
            "hs_analytics_source_data_2",
            "hs_object_id",
            "lastmodifieddate",
            "lead_source",
            "salesforcecontactid",
            "salesforceleadid",
            # campaign_paid.value
            # campaign_paid.timestamp
            "campaign_paid.sourceType",
            "campaign_paid.sourceId",
            # campaign_source.value
            "campaign_source.timestamp",
            "campaign_source.sourceType",
            "campaign_source.sourceId",
        ],
        inplace=True,
    )

    return hs_contact_campaign_df


def get_linkedin_campaign_data():
    # campaign level data
    df = combine_files_in_dir(
        os.path.join("data", "Gtmhub Ads Data", "LinkedIn Ads", "campaigns")
    )

    # Rename main datetime field
    df.rename(columns={"Start Date (in UTC)": "start_date"}, inplace=True)

    # Drop time portion from datetime
    df["start_date"] = df["start_date"].dt.date

    df = df.drop(
        columns=[
            "Account Name",  # All values are 'Gtmhub'
            "Currency",  # All values are 'USD'
            "Campaign Type",  # All values are 'Sponsored Update'
        ]
    )

    # pprint(df.columns.tolist())

    # Create unique identifier for date + campaign in both
    # df["left"] = df["start_date"] + df["Campaign Name"]

    return df


def get_grow_data():
    file_name = "enterprise-mqls-trend-totals-2022-04-13-08_00.xlsx"
    file_name = "all-programs-total-ad-spend-daily-2022-05-19-03_39.xlsx"
    path = os.path.join("data", file_name)
    path = os.path.join("data", "grow", file_name)
    df = pd.read_excel(path)

    # Convet headers to snake case
    df.columns = headers_to_snake_case(df.columns)

    # Drop time portion of datetime field
    df["date"] = df["date"].dt.date

    return df


def get_hubspot_salesforce_day_campaign(hubspot, salesforce):
    # Merge Hubspot with Salesforce
    df = (
        hubspot.merge(salesforce, how="left", left_on="salesforce_id", right_on="id")
        .drop(columns=["id_y"])
        .rename(columns={"id_x": "id"})
    )

    df["is_mql"] = df["became_mql_date"].map(lambda x: 1 if pd.notnull(x) else 0)

    # Create new columns
    df["has_opportunity"] = df["new_business_count"].map(
        lambda x: 1 if pd.notnull(x) and x > 0 else 0
    )
    df["has_meeting"] = df["MeetingCount"].map(
        lambda x: 1 if pd.notnull(x) and x > 0 else 0
    )
    df["has_won_opportunity"] = df["is_new_business_won_count"].map(
        lambda x: 1 if pd.notnull(x) and x > 0 else 0
    )

    # Drop time portion of datetime field
    df["campaign.created_at"] = df["campaign.created_at"].dt.date

    region = (
        df.groupby(["campaign.created_at", "campaign_paid.value", "region"])
        .size()
        .unstack(fill_value=0)
        .add_prefix("region_")
        .reset_index()
    )

    sub_region = (
        df.groupby(["campaign.created_at", "campaign_paid.value", "sub_region"])
        .size()
        .unstack(fill_value=0)
        .add_prefix("sub_region_")
        .reset_index()
    )

    industry = (
        df.groupby(["campaign.created_at", "campaign_paid.value", "industry"])
        .size()
        .unstack(fill_value=0)
        .add_prefix("industry_")
        .reset_index()
    )

    state = (
        df.groupby(["campaign.created_at", "campaign_paid.value", "state"])
        .size()
        .unstack(fill_value=0)
        .add_prefix("state_")
        .reset_index()
    )

    job_function = (
        df.groupby(["campaign.created_at", "campaign_paid.value", "job_function"])
        .size()
        .unstack(fill_value=0)
        .add_prefix("job_function_")
        .reset_index()
    )

    country = (
        df.groupby(["campaign.created_at", "campaign_paid.value", "country"])
        .size()
        .unstack(fill_value=0)
        .add_prefix("country_")
        .reset_index()
    )

    day_campaign = (
        df.groupby(["campaign.created_at", "campaign_paid.value"])
        .agg(
            {
                "id": "count",
                "has_opportunity": "sum",
                "has_meeting": "sum",
                "has_won_opportunity": "sum",
                "is_mql": "sum",
                "new_business_opportunity_amount_sum": "sum",
                "new_business_won_amount_sum": "sum",
            }
        )
        .reset_index()
    )

    # Rename specific columns
    day_campaign.rename(
        columns={
            "id": "user_count",
            "is_mql": "mql_count",
            "has_opportunity": "opportunity_count",
            "has_meeting": "meeting_count",
            "has_won_opportunity": "won_opportunity_count",
        },
        inplace=True,
    )

    # print(industry.columns.tolist())

    # do not include all industry values as fields
    # Get the top 15 used industries and group the remaining together.
    # Get the sum of each industry column.
    industry_counts = {
        header: industry[header].sum()
        for header in industry.columns
        if header.startswith("industry_")
    }

    industry_counts_ranked = sorted(
        industry_counts.items(), key=lambda x: x[1], reverse=True
    )
    industry_fields_ranked = [x[0] for x in industry_counts_ranked]
    # industry_top_15_fields = industry_fields_ranked[:15]
    industry_remaining_fields = industry_fields_ranked[15:]
    # print(industry_counts_ranked)

    # Combine the remaining industry fields
    # Drop the original columns
    industry["industry_remaining"] = industry[industry_remaining_fields].sum(axis=1)
    industry = industry.drop(columns=industry_remaining_fields)

    # print(industry.columns.tolist())

    for _df in (region, sub_region, state, job_function, industry, country):
        day_campaign = day_campaign.merge(
            _df,
            how="left",
            left_on=["campaign.created_at", "campaign_paid.value"],
            right_on=["campaign.created_at", "campaign_paid.value"],
        )

    # Create unique identifier for date + campaign in both
    # day_campaign["right"] = day_campaign["campaign.created_at"] + day_campaign["campaign_paid.value"]

    return day_campaign


def show_details(df):
    print("Dim:", df.shape)
    print()
    print("Sample:")
    print(df.head())
    print()
    print("Columns:")
    pprint(df.columns.tolist())
    print()
    # print(df.info())
    # print(df.describe())


def to_snake_case(string):
    words = string.split(" ")
    lowercase_words = [word.lower() for word in words]

    return "_".join(lowercase_words)


def headers_to_snake_case(headers):
    return [to_snake_case(header) for header in headers]


def clean_combined_data(_df):
    """==================================================
    Clean up data
    =================================================="""
    df = _df.drop(
        columns=[
            "campaign.created_at",  # Duplicate of 'start_date'
            "campaign_paid.value",  # Duplicate of 'Campaign Name'
        ]
    )

    # Rename columns with snake case
    df.columns = headers_to_snake_case(df.columns)

    # Fill numerical fields containing NAs with 0.
    for column_name in df.columns:
        if df[column_name].dtype == "int64":
            df[column_name] = df[column_name].fillna(0)
        elif df[column_name].dtype == "float64":
            df[column_name] = df[column_name].fillna(0.0)

    return df


def merge_hubspot_salesforce(hubspot, salesforce):
    # Merge Hubspot with Salesforce
    df = (
        hubspot.merge(salesforce, how="left", left_on="salesforce_id", right_on="id")
        .drop(columns=["id_y"])
        .rename(columns={"id_x": "id"})
    )

    # df.to_csv(
    #     os.path.join("output", f"hubspot_salesforce_joined.csv"),
    #     index=False,
    # )

    # sys.exit()

    df["is_mql"] = df["became_mql_date"].map(lambda x: 1 if pd.notnull(x) else 0)

    # Create new columns
    df["has_opportunity"] = df["new_business_count"].map(
        lambda x: 1 if pd.notnull(x) and x > 0 else 0
    )
    df["has_meeting"] = df["MeetingCount"].map(
        lambda x: 1 if pd.notnull(x) and x > 0 else 0
    )
    df["has_won_opportunity"] = df["is_new_business_won_count"].map(
        lambda x: 1 if pd.notnull(x) and x > 0 else 0
    )

    # Drop time portion of datetime field
    df["campaign.created_at"] = df["campaign.created_at"].dt.date

    return df

    group_by = ["salesforce_id", "campaign.created_at", "campaign_paid.value"]

    day_campaign = (
        df.groupby(group_by)
        .agg(
            {
                "id": "count",
                "has_opportunity": "sum",
                "has_meeting": "sum",
                "has_won_opportunity": "sum",
                "is_mql": "sum",
                "new_business_opportunity_amount_sum": "sum",
                "new_business_won_amount_sum": "sum",
            }
        )
        .reset_index()
    )

    # Rename specific columns
    day_campaign.rename(
        columns={
            "id": "user_count",
            "is_mql": "mql_count",
            "has_opportunity": "opportunity_count",
            "has_meeting": "meeting_count",
            "has_won_opportunity": "won_opportunity_count",
        },
        inplace=True,
    )

    return day_campaign


def demographics_breakout(hubspot_salesforce):
    df = hubspot_salesforce.copy(deep=True)

    print(df.columns)

    group_by = ["salesforce_id", "campaign.created_at", "campaign_paid.value"]

    demographics = {}

    for field in (
        "region",
        "sub_region",
        "state",
        "job_function",
        "industry",
        "country",
    ):
        demographics[field] = (
            df.groupby(group_by + [field])
            .size()
            .unstack(fill_value=0)
            .add_prefix(f"{field}_")
            .reset_index()
        )

    # do not include all industry values as fields
    # Get the top 15 used industries and group the remaining together.
    # Get the sum of each industry column.
    industry_counts = {
        header: demographics["industry"][header].sum()
        for header in demographics["industry"].columns
        if header.startswith("industry_")
    }

    industry_counts_ranked = sorted(
        industry_counts.items(), key=lambda x: x[1], reverse=True
    )
    industry_fields_ranked = [x[0] for x in industry_counts_ranked]
    # industry_top_15_fields = industry_fields_ranked[:15]
    industry_remaining_fields = industry_fields_ranked[15:]
    # print(industry_counts_ranked)

    # Combine the remaining industry fields
    # Drop the original columns
    demographics["industry"]["industry_remaining"] = demographics["industry"][
        industry_remaining_fields
    ].sum(axis=1)
    demographics["industry"] = demographics["industry"].drop(
        columns=industry_remaining_fields
    )

    day_campaign = (
        df.groupby(group_by)
        .agg(
            {
                "id": "count",
                "has_opportunity": "sum",
                "has_meeting": "sum",
                "has_won_opportunity": "sum",
                "is_mql": "sum",
                "new_business_opportunity_amount_sum": "sum",
                "new_business_won_amount_sum": "sum",
            }
        )
        .reset_index()
    )

    # Rename specific columns
    day_campaign.rename(
        columns={
            "id": "user_count",
            "is_mql": "mql_count",
            "has_opportunity": "opportunity_count",
            "has_meeting": "meeting_count",
            "has_won_opportunity": "won_opportunity_count",
        },
        inplace=True,
    )

    for field in (
        "region",
        "sub_region",
        "state",
        "job_function",
        "industry",
        "country",
    ):
        day_campaign = day_campaign.merge(
            demographics[field],
            how="left",
            left_on=group_by,
            right_on=group_by,
        )

    return df


def is_first_touch(row):
    global unique_combos

    salesforce_id = row["salesforce_id"]
    campaign = row["campaign_paid.value"]

    if salesforce_id == "":
        return False

    combo = (salesforce_id, campaign)

    if combo not in unique_combos:
        unique_combos.append(combo)
        return True

    return False
