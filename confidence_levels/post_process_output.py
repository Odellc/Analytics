import os
import sys
import pandas as pd
# import pyminizip

def split_user_name(user_name):
    word = user_name.strip()

    # If the username is an email address
    if "@" in word:
        at_pos = word.index("@")
        word = word[:at_pos]

        if "." in word:
            array = word.split(".")
            first_name = array[0]
            last_name = "".join(array[1:at_pos])
            return first_name, last_name
        
        return word, ""

    if " " in word:
        array = word.split(" ")
        first_name = array[0]
        last_name = "".join(array[1:])
        return first_name, last_name

    return word, ""


def change_datetime_format(_series, _format):
    '''
    Convert date field to match specified format YYYY/MM/dd
    '''
    _series = pd.to_datetime(
        _series, 
        infer_datetime_format=True, 
        errors='coerce'
    )

    return _series.dt.strftime("%Y/%m/%d")


if __name__ == "__main__":
    original_output_path = os.path.join(
        "output",
        "confidence_levels_2022-03-04 v2.csv"
    )

    df = pd.read_csv(original_output_path)
    results = df["backenduser_name"].map(split_user_name)

    # print(results, type(results))

    df["first_name"] = [row[0] for row in results.tolist()]
    df["last_name"] = [row[1] for row in results.tolist()]

    datetime_headers = [
        "user_datecreated",
        "account_datecreated"
    ]

    for header in datetime_headers:
        df[header] = change_datetime_format(df[header], "%Y/%m/%d")

        print(df[header])

    # print(df.dtypes)
    # print(df["user_datecreated"])
    # # Convert date fields to match specified format YYYY/MM/dd
    # df["user_datecreated"] = pd.to_datetime(
    #     df["user_datecreated"], 
    #     infer_datetime_format=True, 
    #     errors='coerce'
    # )
    # print(df["user_datecreated"])
    # df["user_datecreated"] = df["user_datecreated"].dt.strftime("%Y/%m/%d")

    # print(df["user_datecreated"])

    # df["first_name"], df["last_name"]

    # print(df["first_name"])
    # print(df["last_name"])

    new_output_path = os.path.join("output", "confidence_levels_2022-03-10.csv")
    df.to_csv(new_output_path, index=False)