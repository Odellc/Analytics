from math import comb
import os
import sys
import pandas as pd
import json
from datetime import datetime, timedelta
from dateutil import parser as datetime_parser
from functions import *


if __name__ == "__main__":
    run_date = datetime_string(datetime.now(), "-")

    try:
        arg = sys.argv[1]
    except:
        print("Missing cli arg: (linkedin or grow)")
        sys.exit()

    run_linkedin = False
    run_grow = False
    if arg == "linkedin":
        run_linkedin = True
    elif arg == "grow":
        run_grow = True

    """==================================================
    Read in LinkedIn data
    =================================================="""
    # ad level data
    # ads_df = combine_files_in_dir(
    #     os.path.join("data", "Gtmhub Ads Data", "LinkedIn Ads", "ads", "excel")
    # )

    # Save output for standalone ads data
    # ads_df.to_csv(
    #     os.path.join("output", f"linkedin_ads_by_day_{run_date}.csv"),
    #     index=False
    # )

    print("Reading in LinkedIn campaign data...")

    # campaign level data
    linkedin_campaigns_by_day = get_linkedin_campaign_data()

    """==================================================
    Read in Grow data
    =================================================="""
    grow_df = get_grow_data()

    """==================================================
    Read in Hubspot data
    =================================================="""
    print("Reading in Hubspot data...")

    # Hubspot contact + campaign data
    hubspot = get_hubspot_contact_campaign_data()

    # Remove rows that don't have a value for campaign___paid
    hubspot = hubspot[~hubspot["campaign_paid.value"].isnull()]

    # show_details(hubspot)

    """==================================================
    Read in Salesforce data
    =================================================="""
    print("Reading in Salesforce data...")

    # Salesforce prospects data (mql, meetings, opportunities)
    salesforce = pd.read_csv(
        os.path.join("data", "salesforce_prospects_data_for_ads_v4.csv")
    )

    """==================================================
    Combine all data
    =================================================="""
    # hs_sf_campaigns_by_day = get_hubspot_salesforce_day_campaign(hubspot, salesforce)

    # HOW DOES THIS WORK WITH CAMPAIGNS THAT SPAN SOURCES?

    if run_linkedin:
        og = set(hubspot["campaign_source.value"].unique())

        # Keep only LinkedIn data
        hubspot = hubspot[
            ~hubspot["campaign_source.value"].str.contains("linkedin", case=False)
        ]

        # linkedin_only = set(hubspot["campaign_source.value"].unique())

        # print(og - linkedin_only)

        print(
            hubspot[
                hubspot["campaign_paid.value"].str.contains("linkedin", case=False)
            ]["campaign_paid.value"].nunique()
        )

        sys.exit()

    # The param hubspot in this version of the script has all campaign dates
    # Filter out all proceeding touchpoints and store for later.
    hubspot_first_campaigns = hubspot.sort_values(
        ["id", "campaign.created_at"]
    ).drop_duplicates(subset="id", keep="first", ignore_index=True)

    print("All rows:", hubspot.shape)
    print("First rows:", hubspot_first_campaigns.shape)
    # sys.exit()

    print("Merging Hubspot + Salesforce...")

    # Unique headers for first touch and all touch campaigns
    unique_headers = [
        "user_count",
        "opportunity_count",
        "meeting_count",
        "won_opportunity_count",
        "mql_count",
        "new_business_opportunity_amount_sum",
        "new_business_won_amount_sum",
    ]

    # All hubspot data
    hs_sf_campaigns_by_day = get_hubspot_salesforce_day_campaign(hubspot, salesforce)
    hs_sf_campaigns_by_day.rename(
        columns={old_header: "all_" + old_header for old_header in unique_headers},
        inplace=True,
    )

    # Only first touch point hubspot data
    hs_sf_campaigns_by_day_first = get_hubspot_salesforce_day_campaign(
        hubspot_first_campaigns, salesforce
    )
    hs_sf_campaigns_by_day_first.rename(
        columns={old_header: "first_" + old_header for old_header in unique_headers},
        inplace=True,
    )

    print(hs_sf_campaigns_by_day.columns.tolist())
    print(hs_sf_campaigns_by_day_first.columns.tolist())

    print("Merging first campaign + all campaign Hubspot/Salesforce data...")
    hs_sf_campaigns_by_day = hs_sf_campaigns_by_day.merge(
        hs_sf_campaigns_by_day_first,
        how="left",
        left_on=["campaign.created_at", "campaign_paid.value"],
        right_on=["campaign.created_at", "campaign_paid.value"],
    )

    # DEDUPE HEADERS
    # Once merged, Pandas converts headers with same names to avoid collussion.
    # The first Df headers are appended with postfix _x
    # The second Df headers are appended with postfix _y

    # Remove _x from headers
    _x_headers = [x for x in hs_sf_campaigns_by_day.columns if x.endswith("_x")]
    hs_sf_campaigns_by_day = hs_sf_campaigns_by_day.rename(
        columns={old: old[:-2] for old in _x_headers}
    )

    # Delete headers containing _y
    _y_headers = [x for x in hs_sf_campaigns_by_day.columns if x.endswith("_y")]
    hs_sf_campaigns_by_day.drop(columns=_y_headers, inplace=True)

    print(hs_sf_campaigns_by_day.head())

    pprint(hs_sf_campaigns_by_day.columns.tolist())
    # sys.exit()

    combined_campaign_day = linkedin_campaigns_by_day.merge(
        hs_sf_campaigns_by_day,
        how="left",
        left_on=["start_date", "Campaign Name"],
        right_on=["campaign.created_at", "campaign_paid.value"],
    )

    combined_grow = grow_df.merge(
        hs_sf_campaigns_by_day,
        how="left",
        left_on=["date", "campaign"],
        right_on=["campaign.created_at", "campaign_paid.value"],
    )

    combined_campaign_day = clean_combined_data(combined_campaign_day)
    combined_grow = clean_combined_data(combined_grow)

    """==================================================
    Clean up data
    ==================================================
    combined_campaign_day.drop(
        columns=[
            "campaign.created_at",  # Duplicate of 'start_date'
            "campaign_paid.value",  # Duplicate of 'Campaign Name'
        ],
        inplace=True,
    )

    # Rename columns with snake case
    combined_campaign_day.columns = headers_to_snake_case(combined_campaign_day.columns)

    # Fill numerical fields containing NAs with 0.
    for column_name in combined_campaign_day.columns:
        if combined_campaign_day[column_name].dtype == "int64":
            combined_campaign_day[column_name] = combined_campaign_day[
                column_name
            ].fillna(0)
        elif combined_campaign_day[column_name].dtype == "float64":
            combined_campaign_day[column_name] = combined_campaign_day[
                column_name
            ].fillna(0.0)
        # print(combined_campaign_day[column_name].dtype)

    # sys.exit()

    print("=" * 40)
    show_details(combined_campaign_day)
    """

    # show_details(combined_grow)

    # combined_grow.drop(
    #     columns=[
    #         "campaign.created_at", # Duplicate of 'start_date'
    #         "campaign_paid.value" # Duplicate of 'Campaign Name'
    #     ],
    #     inplace=True
    # )

    # Create unique identifier for date + campaign in both
    # campaigns_df["left"] = campaigns_df["start_date"] + campaigns_df["campaign_paid.value"]
    # day_campaign["right"] = day_campaign["campaign.created_at"] + day_campaign[""]

    # Join Hubspot / Salesforce data to the main dataset.
    # main_df = campaigns_df.merge(
    #     day_campaign,
    #     how="left",
    #     left_on="salesforce_id",
    #     right_on="Id"
    # )

    # main_df = df.merge(
    #     hs_sf_contact_campaign_df,
    #     how="left",
    #     left_on="Campaign Name",
    #     right_on="campaign_paid.value"
    # )

    if run_linkedin:
        # Export Linkedin campaign by day
        combined_campaign_day.to_csv(
            os.path.join("output", f"combined_linkedin_campaign_by_day_{run_date}.csv"),
            index=False,
        )
    elif run_grow:
        # Export Grow campaign by day
        combined_grow.to_csv(
            os.path.join("output", f"combined_grow_campaign_by_day_{run_date}.csv"),
            index=False,
        )
