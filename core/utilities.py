from os import PathLike
from pathlib import Path
from queue import Queue
import time
import os
from datetime import datetime, UTC
import functools
from typing import Iterable

import pandas as pd

from core.db import PostgresDB, DB_NAME


def runtime_counter(func):
    """A decorator for measuring and logging a function's runtime."""

    @functools.wraps(func)
    #def wrapper(*args, **kwargs):
        # start_time = time.time()
        
        # # Run the decorated function
        # result = func(*args, **kwargs)

        # # Calculate elapsed time
        # elapsed_time = time.time() - start_time
        # hours, remainder = divmod(elapsed_time, 3600)
        # minutes, seconds = divmod(remainder, 60)
        # milliseconds = (elapsed_time % 1) * 1000
        # # Log the runtime details
        # print(
        #     f"{func.__name__} runtime: "
        #     f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{int(milliseconds):03}"
        # )
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        print(f"Function {func.__name__} executed in {elapsed_time:.2f} seconds.")
        return result
    return wrapper



def get_utc_timestamp():
    curr_dt = datetime.now(UTC)
    timestamp = int(round(curr_dt.timestamp()))
    return timestamp



class PandasHelper:
    def __init__(self, path_to_save : PathLike | str = 'data'):
        """
        Initialize the PandasHelper class.

        Parameters:
            path_to_save (PathLike | str): The path to save the CSV file within the project.
        """
        self.path_to_save = path_to_save
        os.makedirs(self.path_to_save, exist_ok=True)

    def merge_from_csv_files(self, csv_files: list[str | Path]) -> pd.DataFrame:
        """
        Merge multiple CSV files into a single DataFrame.

        Parameters:
            csv_files (list[str | Path]): List of paths to the CSV files to merge.

        Returns:
            pd.DataFrame: A DataFrame containing the merged and deduplicated data.
        """
        data_frames = []

        # Read each file into a DataFrame
        for file in csv_files:
            try:
                df = pd.read_csv(file)
                data_frames.append(df)
            except (FileNotFoundError, pd.errors.EmptyDataError):
                print(f"Warning: Could not read {file}. Skipping.")

        # Concatenate and drop duplicates based on 'id'
        try:
            merged_df = pd.concat(data_frames, ignore_index=True).drop_duplicates(subset='id', keep='last')
            print(f"Merged {len(csv_files)} files. Total records after deduplication: {len(merged_df)}")
            return merged_df
        except ValueError as e:
            print(f"Error merging files: {e}")
            return pd.DataFrame()


    def save_data_to_csv_file(
            self,
            df: pd.DataFrame,
            output_file_name: PathLike | str,
            create_new_file: bool = True
    ) -> None:
        """
        Save the merged DataFrame to a file, either creating a new file or appending to an existing file.

        Parameters:
            df (pd.DataFrame): The DataFrame to save.
            output_file_name (str): Path to the output file.
            create_new_file (bool): Whether to create a new file or append to an existing one.
        """
        if df.empty:
            print("No data to save. Exiting...")
            return

        filepath = os.path.join(self.path_to_save, output_file_name)

        if create_new_file:
            if not os.path.exists(filepath):
                # Save as a new file
                print(f"New file created: {filepath}. Total records: {len(df)}")
            else:
                print(f"File already exists: {filepath}. Rewriting to it.")
            df.to_csv(filepath, index=False)
        else:
            # Append to the existing file while maintaining uniqueness
            try:
                existing_df = pd.read_csv(filepath)
                combined_df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates(
                    subset='id', keep='last'
                )
                combined_df.to_csv(filepath, index=False)
                print(f"Appended and updated file: {filepath}. Total records: {len(combined_df)}")
            except (FileNotFoundError, pd.errors.EmptyDataError):
                print(f"Error: Could not read {filepath}. Saving as a new file.")
                df.to_csv(output_file_name, index=False)








def dedupe(items, key=None): 
    seen = set()
    for item in items:
        val = item if key is None else key(item)
        if val not in seen:
            yield item 
            seen.add(val)


def return_unique_records(items) -> list:
    records = list(dedupe(items, key=lambda d: d['id']))
    print (f"Returning {len(records)} unique records.")
    return records




def merge_csv_files(input_folder, output_file_name):
    # Get all CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    # Create an empty list to hold DataFrames
    data_frames = []

    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        data_frames.append(df)

    # Concatenate all DataFrames into one
    merged_df = pd.concat(data_frames, ignore_index=True)

    # Optional: Drop duplicates based on a specific column (e.g., 'id')
    merged_df.drop_duplicates(subset='id', keep='first', inplace=True)

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(output_file_name, index=False)
    print(f"Merged CSV file saved to {output_file_name}")


# def clean_data(filename):
#     df = pd.read_csv(filename)
#
#     df.loc[df['price_for'] == 'на продажу', 'price_for'] = None
#     print(df.dtypes)


def save_to_db_from_csv(filename):
    pass




