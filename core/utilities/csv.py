import os
from os import PathLike
from pathlib import Path

import pandas as pd


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