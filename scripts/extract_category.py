import argparse
import csv
import sys
import polars as pl
from polars import DataFrame
from datetime import datetime, timedelta
from collections import defaultdict
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(curr_dir, "..", "data")

def load_character(input_file):
    df = pl.read_csv(input_file, has_header=True)
    return df

def merge_dataframes(dfs):
    merged_df = pl.concat(dfs)
    return merged_df

def make_category_csv_files(merged_df):
    merged_df = merged_df.sort("Release Date")
    categories = merged_df["Unique Coding"].unique().to_list()
    for category in categories:
        category_df = merged_df.filter(pl.col("Unique Coding") == category)
        category_file_name = category.replace("/", "_") + ".csv"
        output_file = os.path.join(data_dir, "Categories", category_file_name)
        category_df.write_csv(output_file)

def main():
    filenames = [
        "Petyr_Baelish.csv",
        "Sam_Tarly.csv",
        "Theon_Greyjoy.csv"
    ]

    dataframes = []
    for filename in filenames:
        input_path = os.path.join(data_dir, "Annotated", filename)
        df = load_character(input_path)
        dataframes.append(df)

    merged_df = merge_dataframes(dataframes)
    make_category_csv_files(merged_df)


if __name__ == "__main__":
    main()