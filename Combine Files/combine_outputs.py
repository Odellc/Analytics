import os
import sys
import pandas as pd
import openpyxl
from csv import reader

#Constant

ABS_PATH = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])

# Anatomy of an Amplitude export
# A1 => URL to the query [This may not always be the case?]
# A2 => type of metric (Uniques / Totals / % Active / Average, etc.)
# A3 - A(x) => Event Name
# A(x + 1) => (blank row)
# A(x + 2) => header row
# A(x + 3) => first row of data
# A(y) => (blank row) 


def main():
    path = os.path.join("event_segmentation_2021_quarterly_DL", "grouped_by_account")

    print(f"Reading files from directory: {path}")

    for csv_file_name in os.listdir(path):
        file_path = os.path.join(ABS_PATH, path, csv_file_name)

        print(f"\t{csv_file_name}")
        
        with open(file_path, "r") as csv_file:
            csv_reader = reader(csv_file)
            header = next(csv_reader)

            if header != None:
                for idx, row in enumerate(csv_reader):

                    if idx < 20:
                        print(len(row))
                        print(row)
                    # sys.exit()

        sys.exit()


if __name__ == "__main__":
    main()