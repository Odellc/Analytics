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


def select_sample(population, sample_size, last_group=False):
    accounts_sampled = []
    users_sampled = []
    pop = population[:]

    if last_group:
        return population, []

    # While sample not fulfilled.
    while len(users_sampled) < sample_size:
        account, user = sample(pop, 1)[0]

        if account in accounts_sampled:
            # Repick unless impossible to get the whole sample from
            # non-repeating accounts
            population_unique_accounts = set([x[0] for x in pop])
            sample_unique_accounts = set(accounts_sampled)

            # There is at least 1 unique account in population
            if len(population_unique_accounts - sample_unique_accounts) > 0:
                continue

        accounts_sampled.append(account)
        users_sampled.append([account, user])

        # Update population
        pop = [row for row in pop if row not in users_sampled]

    return users_sampled, pop


if __name__ == "__main__":
    last_sample_version = 2
    sample_size = 500

    # Read in existing users after most recent sample
    path = os.path.join(f"remaining_users_after_v{last_sample_version}.csv")
    remaining_user_data = pd.read_csv(path)
    print("Users remaining since last run:", remaining_user_data.shape[0])
    pprint(remaining_user_data.columns.tolist())

    # Fix date from Excel date to datetime
    remaining_user_data["user.date_created"] = remaining_user_data["user.date_created"].map(
        lambda x: xldate_as_datetime(x, 0)
    )

    # Read in users from tag output to make sure we do not sample from those individuals
    tag_user_df = pd.read_csv("tag_filtering_users_data_2022-04-07.csv")
    tag_users = tag_user_df["event.user_id"].tolist()

    # Don't include tag user that had previously been surveyed
    new_remaining_users_df = remaining_user_data[
        ~remaining_user_data["user.id"].isin(tag_users)
    ]

    # Remove duplicate rows
    new_remaining_users_df = new_remaining_users_df.drop_duplicates(keep="first")
    print("# of users left:", new_remaining_users_df.shape[0])

    # Convert data to list to reduce overhead
    population = [
        [row["account.id"], row["user.id"]]
        for i, row in new_remaining_users_df.iterrows()
    ]

    population_size = new_remaining_users_df.shape[0]

    num_groups_needed = population_size // sample_size + 1

    print(num_groups_needed)

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
        N = len(population)
        last_group = False if N > sample_size else True
        n = sample_size if not last_group else N

        users_sampled, population = select_sample(population, n, last_group)
        
        # Add group column
        users_sampled = [row + [group_number] for row in users_sampled]
        print(len(users_sampled))

        all_users_sampled.extend(users_sampled)
        # population = [row for row in population if row not in users_sampled]

        # pprint(all_users_sampled)
        

    # Select
    df = new_remaining_users_df.merge(
        pd.DataFrame(all_users_sampled, columns=["account.id", "user.id", "sample_group"]),
        how="left",
        left_on="user.id",
        right_on="user.id"
    )

    # Save output of all samples and their groups
    export_date = datetime_string(datetime.datetime.now(), '-')
    file_name = f"all_remaining_users_with_sample_groups_of_size_{sample_size}_as_of_{export_date}_v2.csv"
    df.to_csv(file_name, index=False)