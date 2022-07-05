import os
import sys
from random import sample
from tokenize import group
import pandas as pd
import datetime
from xlrd import xldate_as_datetime
from pprint import pprint
from functions_2 import *


def epoch_to_datetime(epoch, units="ms"):
    # There was a negative value in users.json
    # This might be a problem elsewhere.
    timestamp = abs(epoch)

    try:
        if units == "ms":
            return datetime.datetime.fromtimestamp(0) + datetime.timedelta(milliseconds=timestamp)

        if units == "s":
            return datetime.datetime.fromtimestamp(0) + datetime.timedelta(seconds=timestamp)
    except Exception as e:
        print(timestamp)
        print("Error with epoch")
        
        # sys.exit()
        return datetime.datetime.fromtimestamp(0)


def select_sample(data, sample_size):
    pass


if __name__ == "__main__":
    project_name = "whiteboard"
    last_sample_version = 2
    desired_sample_size = 500

    # Read in existing users after most recent sample
    path = os.path.join(f"remaining_users_after_v{last_sample_version}.csv")
    remaining_user_data = pd.read_csv(path)

    print(remaining_user_data.head())

    print(remaining_user_data.shape)
    pprint(remaining_user_data.columns.tolist())

    remaining_user_data["user.date_created"] = remaining_user_data["user.date_created"].map(
        lambda x: xldate_as_datetime(x, 0)
    )
    print(remaining_user_data["user.date_created"].sort_values().head())

    print("Current # of users:", remaining_user_data.shape[0])

    # Read in users from tag output to make sure we do not sample from those individuals
    tag_user_df = pd.read_csv("tag_filtering_users_data_2022-04-07.csv")
    # pprint(tag_user_df.columns.tolist())
    tag_users = tag_user_df["event.user_id"].tolist()

    new_remaining_users_df = remaining_user_data[
        ~remaining_user_data["user.id"].isin(tag_users)
    ]

    print("# of users left:", new_remaining_users_df.shape[0], len(new_remaining_users_df["user.id"].unique()))

    new_remaining_users_df = new_remaining_users_df.drop_duplicates(keep="first")

    print(new_remaining_users_df.shape)

    num_users_remaining = new_remaining_users_df.shape[0]

    num_groups_needed = num_users_remaining // desired_sample_size + 1

    print(num_groups_needed)

    # for user in new_remaining_users_df["user.id"].unique():
    #     _df = new_remaining_users_df[new_remaining_users_df["user.id"] == "5e4f8805d02bfb0001a8d24c"]

    #     for index, row in _df.iterrows():
    #         print(row)
    #         print()

    #     # if _df.shape[0] > 1:
    #     #     print(_df)
    #     #     print(user)
    # sys.exit()

    print("=" * 40)

    # Sample users, but no more than 1 user per account
    all_users_sampled = [] # list of df

    for group_number in range(1, num_groups_needed + 1):
        accounts_sampled = []
        users_sampled = []

        print("Sampling group", group_number)

        """
        Possible cases when selecting a sample:
        - account is already taken
            - set(existing accounts) - set(taken accounts) is empty, meaning everything else needs to be a duplicate account
        - account is not taken
            - user the sampled user 
        """
        num_remaining = new_remaining_users_df.shape[0]

        # Remaining population is larger sample size
        if num_remaining > desired_sample_size:
            # Loop until sample size met
            while len(users_sampled) < desired_sample_size:
                user = sample(new_remaining_users_df["user.id"].tolist(), 1)[0]
                account = new_remaining_users_df[new_remaining_users_df["user.id"] == user]["account.id"].iloc[0]

                existing_accounts = new_remaining_users_df["account.id"]

                if account in accounts_sampled:
                    # Already exists in sampled; Decide whether to resample or let the sample slide
                    # If there are no more unique accounts, allow the sample to be added
                    if len(set(existing_accounts) - set(accounts_sampled)) == 0:
                        accounts_sampled.append(account)
                        users_sampled.append(user)

                    # There are still unique accounts. Reselect.
                    else:
                        continue
                else:
                    # Add the item as sampled
                    accounts_sampled.append(account)
                    users_sampled.append(user)

        # Less than or equal to desired sample size remains.
        else:
            # new_remaining_users_df["sample_group"] = f"Sample Group {group_number}"
            users_sampled = new_remaining_users_df["user.id"].tolist()

        # Add rows of user's sampled to collection
        new_sample_group = new_remaining_users_df[new_remaining_users_df["user.id"].isin(users_sampled)]
        new_sample_group["sample_group"] = new_sample_group["user.id"].map(lambda x: f"Sample Group {group_number}")
        all_users_sampled.append(new_sample_group)

        # Update exisitng population
        new_remaining_users_df = new_remaining_users_df[~new_remaining_users_df["user.id"].isin(users_sampled)]

    # Merge sample groups
    df = pd.concat(all_users_sampled)

    # Save output of all samples and their groups
    export_date = datetime_string(datetime.datetime.now(), '-')
    file_name = f"all_remaining_users_with_sample_groups_of_size_{desired_sample_size}_as_of_{export_date}.csv"
    df.to_csv(file_name, index=False)