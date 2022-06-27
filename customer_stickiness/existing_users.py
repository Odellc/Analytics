import os
import sys
import pandas as pd

# Read in existing sample
df = pd.read_csv("sample_users_180+days_data_2022-04-07.csv")

print(df.info())