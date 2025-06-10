import pandas as pd
import os
import re

class FileHandler:
    @staticmethod
    def read_csv(file_path):
        # Load a CSV file into a DataFrame
        return pd.read_csv(file_path)

    @staticmethod
    def normalize_columns(columns):
        # Clean column names: lowercase, remove special chars, replace spaces with underscores
        return [re.sub(r"[^\w]", "", col.strip().lower().replace(" ", "_")) for col in columns]

    @staticmethod
    def write_csv(path, df):
        # Save DataFrame to CSV
        df.to_csv(path, index=False)

    @staticmethod
    def get_files(path, target=".csv"):
        # Return list of files that match target extension
        if os.path.isfile(path) and path.lower().endswith(target):
            return [path]
        if os.path.isdir(path):
            return [
                os.path.join(path, f)
                for f in os.listdir(path)
                if f.lower().endswith(target)
            ]
        return []

    @staticmethod
    def file_exists(path):
        # Check if file exists at the given path
        return os.path.isfile(path)

    @staticmethod
    def normalize_name(name, max_length=100):
        # Normalize a name: strip extension, lowercase, replace separators, trim non-alphanumerics
        name = os.path.splitext(os.path.basename(name))[0].lower()
        name = re.sub(r"[ \-:\.]+", "_", name)
        name = re.sub(r"[^\w]", "", name)
        return name[:max_length].strip("_")

    @staticmethod
    def split(file_path, output_path, split_type, split_value):
        # Split a CSV into multiple files by row count or column value
        df = FileHandler.read_csv(file_path)
        base_name = FileHandler.normalize_name(os.path.splitext(os.path.basename(file_path))[0])

        if split_type == "row_count":
            # Split by fixed number of rows
            try:
                count = int(split_value)
                if count <= 0:
                    raise ValueError("Split size must be greater than zero.")
            except ValueError:
                raise ValueError("Invalid split value for row count.")

            for i in range(0, len(df), count):
                chunk = df.iloc[i:i + count]
                chunk_index = i // count + 1
                chunk_name = FileHandler.normalize_name(f"{base_name}_part_{chunk_index}")
                full_path = os.path.join(output_path, f"{chunk_name}.csv")
                FileHandler.write_csv(full_path, chunk)

        elif split_type == "column_value":
            # Split by unique values in a specific column
            if split_value not in df.columns:
                raise ValueError(f"Column '{split_value}' not found in file.")

            for val, group in df.groupby(split_value):
                safe_val = FileHandler.normalize_name(str(val))
                chunk_name = FileHandler.normalize_name(f"{base_name}_{safe_val}")
                full_path = os.path.join(output_path, f"{chunk_name}.csv")
                FileHandler.write_csv(full_path, group)

        else:
            raise ValueError("Unsupported split type.")
