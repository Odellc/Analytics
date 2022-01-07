import os
import sys

import pandas as pd
from pathlib import Path

home = str(Path.home())

de_path = os.path.join(home,"OneDrive - Gtmhub Ltd", "Analytics", "Data Engineering")

df = pd.read_csv(os.path.join(de_path, "CustomerMRRChange2021.csv"))

def main():

    for i, r in df.iterrows():
        print(i, r["Subscription ID"])
        print(i, r["Current MRR"])
        sys.exit()

if __name__ == '__main__':

    main()