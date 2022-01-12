import os
import sys
import json
import pandas as pd

def concatenate(array):
    return "(" + ", ".join([
        f"'{item}'"
        for item in array
    ]) + ")"


if __name__ == "__main__":
    file_path = os.path.join("data", "insight_results_v2.json")

    with open(file_path, "r") as json_file:
        json_data = json.load(json_file)

    new_output = []

    for row_data in json_data["default"]:
        new_output.append(
            [
                row_data["id"], 
                row_data["email"],
                row_data["account_id"]
            ] 
        )

    df = pd.DataFrame(new_output, columns=["id", "email", "account_id"])

    output_path = os.path.join("output", "insight_results_v2.xlsx")
    df.to_excel(output_path, index=False)

    print("Concatonated account_id list:")
    
    concatenated_account_ids = concatenate(df["account_id"].tolist())

    print(concatenated_account_ids)

    file_path = os.path.join("data", "customer_service_designated_account-ds&a_results.json")
    with open(file_path, "r") as json_file:
        json_data = json.load(json_file)

    customer_success_df = pd.DataFrame(json_data["default"])

    output_df = pd.merge(
        df, 
        customer_success_df, 
        how="inner", 
        left_on="account_id", 
        right_on="chargebee__c"
    )

    output_df.to_excel(
        os.path.join("output", "Mobile Users with Customer Success Managers.xlsx"),
        index=False
    )