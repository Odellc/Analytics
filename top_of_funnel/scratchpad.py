import os
import sys
import pandas as pd

"=========="


def compare_data(path):
    # {<column_view>: df}
    data = {}

    print("=" * 40)

    for file_name in os.listdir(path):
        column_view = file_name.split(".")[0]

        print(column_view)

        source = os.path.join(path, file_name)
        df = pd.read_csv(source, sep="\t", encoding="utf-16", skiprows=5)

        data[column_view] = df

    # # Source 2: column_view: "Campaign Group: All Columns For Export"
    # source_2 = os.path.join(root_ads_dir, linkedin_dir, "account_503530200_campaign_performance_report (1).csv")
    # df_2 = pd.read_csv(source_2, sep="\t", encoding="utf-16", skiprows=5)
    # print(df_2)
    # print(df_2.columns)

    keys = sorted(data.keys())
    key_1, key_2 = keys
    columns_1, columns_2 = columns = [set(data[key].columns) for key in keys]
    len_1, len_2 = [len(x) for x in columns]

    print(len_1, len_2, len_1 - len_2)
    print(columns_1 - columns_2)
    print(columns_2 - columns_2)

    print("=" * 40)
    print()


if __name__ == "__main__":
    root_ads_dir = "Gtmhub Ads Data"

    """======================================================================
    LinkedIn Data
    ======================================================================"""
    linkedin_dir = "LinkedIn Ads"

    """============================================================
    Campaign Group
    ============================================================"""
    ad_section = "campaign_group"

    """==================================================
    Report Type: Campaign Performance
    =================================================="""
    # Comparing sources to see if they are the same.
    # Want to ensure all data sources are downloaded
    # and nothing is missing.
    report_type = "campaign_performance"
    report_types = os.listdir(os.path.join(root_ads_dir, linkedin_dir, ad_section))

    # for file_name in report_types:
    #     if "demographics" in file_name:
    #         continue

    #     report_type = file_name.split(".")[0]

    #     current_path = os.path.join(root_ads_dir, linkedin_dir, ad_section, report_type)

    #     compare_data(current_path)

    # {<column_view>: df}
    data = {}

    source_1 = os.path.join(
        root_ads_dir, linkedin_dir, ad_section, "ad_performance", "all_columns.csv"
    )
    # # Source 2: column_view: "Campaign Group: All Columns For Export"
    # source_2 = os.path.join(root_ads_dir, linkedin_dir, "account_503530200_campaign_performance_report (1).csv")
    # df_2 = pd.read_csv(source_2, sep="\t", encoding="utf-16", skiprows=5)
    # print(df_2)
    # print(df_2.columns)

    keys = sorted(data.keys())
    key_1, key_2 = keys
    columns_1, columns_2 = columns = [set(data[key].columns) for key in keys]
    len_1, len_2 = [len(x) for x in columns]

    print(len_1, len_2, len_1 - len_2)
    print(columns_1 - columns_2)
    print(columns_2 - columns_2)
