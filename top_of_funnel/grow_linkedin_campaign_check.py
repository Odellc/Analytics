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
    input_file_date = datetime(2022, 5, 17).strftime("%Y-%m-%d")

    print("Reading in combined LinkedIn campaign data...")
    combined_linkedin_campaign_data_path = os.path.join(
        "output",
        "linkedin_campaign",
        f"combined_linkedin_campaign_by_day_{input_file_date}.csv",
    )
    linkedin_data = pd.read_csv(combined_linkedin_campaign_data_path)

    print("Reading in combined grow data...")
    combined_grow_data_path = os.path.join(
        "output", "grow", f"combined_grow_campaign_by_day_{input_file_date}.csv"
    )
    grow_data = pd.read_csv(combined_grow_data_path)

    print(linkedin_data.head())
    print(linkedin_data.columns)
    print(linkedin_data.shape)

    print(grow_data.head())
    print(grow_data.columns)
    print(grow_data.shape)

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

    linkedin_data.rename(
        columns={old_header: "li." + old_header for old_header in unique_headers},
        inplace=True,
    )

    grow_data = grow_data.rename(
        columns={old_header: "grow." + old_header for old_header in unique_headers},
    )

    pprint({old_header: "grow." + old_header for old_header in unique_headers})

    print([x for x in grow_data.columns if "grow" in x])

    df = grow_data.merge(
        linkedin_data,
        how="outer",
        left_on=["date", "campaign"],
        right_on=["start_date", "campaign_name"],
    )

    df["start_date"] = df["start_date"].combine_first(df["date"])
    df["campaign_name"] = df["campaign_name"].combine_first(df["campaign"])

    df.drop(columns=["date", "campaign"], inplace=True)

    columns_to_keep = []
    for column in df.columns:
        if (
            column.startswith("all_")
            or column.startswith("first_")
            or column in ["start_date", "date", "campaign", "campaign_name"]
        ):
            columns_to_keep.append(column)

    df = df.loc[:, columns_to_keep]

    print(df.head())
    print(df.shape)

    df.to_csv(
        os.path.join("output", "verification", f"outer_join_grow_li_{run_date}.csv"),
        index=False,
    )
