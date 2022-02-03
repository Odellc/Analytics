import os
import numpy as np
import matplotlib as plt
import pandas as pd


THIS_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
os.chdir("../..")

# ABS_PATH = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-2])
# print(ABS_PATH)

data_path = os.path.join(os.curdir,"Machine Learning A-Z (Codes and Datasets)","Data", "preproecssing" )
print(data_path)
dataset = pd.read_csv(os.path.join(data_path,"Data.csv"))
# print(dataset)

def main():
    pass


if __name__ == "__main__":
    main()