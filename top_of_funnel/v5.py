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

    """==================================================
    Read in Hubspot data
    =================================================="""
    print("Reading in Hubspot data...")

    # Hubspot contact + campaign data
    hubspot = get_hubspot_contact_campaign_data()

    print("Hubspot info:")
    print(hubspot.shape)
    # print(hubspot["campaign_paid.value"].unique())

    # hubspot.to_csv(
    #     os.path.join("output", f"hubspot_full.csv"),
    #     index=False,
    # )

    # Remove rows that don't have a value for campaign___paid
    hubspot = hubspot[
        ~hubspot["campaign_paid.value"].isnull()
        & (hubspot["campaign_paid.value"] != "")
    ]
    # do i need to add not empty string?
    print("after filter:")
    print(hubspot.shape)
    # print(hubspot["campaign_paid.value"].unique())

    unique_combos = []
    first_touches = []
    for index, row in hubspot.iterrows():
        salesforce_id = row["salesforce_id"]
        campaign = row["campaign_paid.value"]
        combo = (salesforce_id, campaign)

        if salesforce_id == "":
            first_touches.append(False)
        elif combo not in unique_combos:
            unique_combos.append(combo)
            first_touches.append(True)
        else:
            first_touches.append(False)

    hubspot["is_first_touch"] = first_touches

    print(hubspot)
    sys.exit()

    # hs = hubspot[
    #     hubspot["campaign_paid.value"]
    #     == "lead-multi-axis-ebook-cso-companies-noam-linkedin"
    # ]

    # for index, row in hs.iterrows():
    #     print(row)

    # show_details(hubspot)

    # Sample
    # ultim. okrs - e-book - download - lp - lal
    # 0034W00002hPw9RQAS
    # sample = hubspot[
    #     (hubspot["salesforce_id"] == "0034W00002hPw9RQAS")
    #     & (
    #         hubspot["campaign_paid.value"]
    #         == "ultim. okrs - e-book - download - lp - lal"
    #     )
    # ]

    # sys.exit()

    """==================================================
    Read in Salesforce data
    =================================================="""
    print("Reading in Salesforce data...")

    # Salesforce prospects data (mql, meetings, opportunities)
    salesforce = pd.read_csv(
        os.path.join("data", "salesforce", "salesforce_prospects_data_for_ads_v5.csv")
    )

    """==================================================
    Combine all data
    =================================================="""
    # hs_sf_campaigns_by_day = get_hubspot_salesforce_day_campaign(hubspot, salesforce)

    # The param hubspot in this version of the script has all campaign dates
    # Filter out all proceeding touchpoints and store for later.
    hubspot_first_campaigns = hubspot.sort_values(
        ["salesforce_id", "campaign.created_at", "campaign_paid.value"]
    ).drop_duplicates(
        subset=["salesforce_id", "campaign_paid.value"], keep="first", ignore_index=True
    )

    print("adding column")
    hubspot_first_campaigns.loc[:, "is_first_touch"] = hubspot_first_campaigns.apply(
        lambda x: True
    )

    # Drop all other columns
    print("Keeping only specified columns")
    hubspot_first_campaigns = hubspot_first_campaigns.loc[
        :,
        [
            "salesforce_id",
            "campaign.created_at",
            "campaign.created_at",
            "is_first_touch",
        ],
    ]

    print(hubspot.head())
    print(hubspot.columns)
    print(hubspot.shape)

    # Merge with other hubspot data ALL
    hubspot = hubspot.merge(
        hubspot_first_campaigns,
        how="left",
        left_on=["salesforce_id", "campaign.created_at", "campaign_paid.value"],
        right_on=["salesforce_id", "campaign.created_at", "campaign_paid.value"],
    )

    print(hubspot.head())
    print(hubspot.columns)
    print(hubspot.shape)

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
    hs_sf_campaigns_by_day = merge_hubspot_salesforce(hubspot, salesforce)
    # sys.exit()
    hs_sf_campaigns_by_day.rename(
        columns={old_header: "all_" + old_header for old_header in unique_headers},
        inplace=True,
    )

    # Only first touch point hubspot data
    hs_sf_campaigns_by_day_first = merge_hubspot_salesforce(
        hubspot_first_campaigns, salesforce
    )
    sys.exit()
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
        left_on=["salesforce_id", "campaign.created_at", "campaign_paid.value"],
        right_on=["salesforce_id", "campaign.created_at", "campaign_paid.value"],
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

    hs_sf_campaigns_by_day = demographics_breakout(hs_sf_campaigns_by_day)

    print(hs_sf_campaigns_by_day.head())

    pprint(hs_sf_campaigns_by_day.columns.tolist())
    # sys.exit()

    hs_sf_campaigns_by_day = clean_combined_data(hs_sf_campaigns_by_day)

    # Export Grow campaign by day
    hs_sf_campaigns_by_day.to_csv(
        os.path.join("output", f"hubspot_salesforce_prospect_campaign_{run_date}.csv"),
        index=False,
    )
