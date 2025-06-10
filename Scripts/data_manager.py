import os
from Scripts.cli import CLI
from Scripts.database import Database
from Scripts.file_handler import FileHandler

class DataManager:
    def __init__(self, db: Database):
        self.db = db

    def handle_upload(self):
        # Let user select .csv file(s) to upload
        files = CLI.get_files(".csv")

        rename_action = True
        while True:
            if files and len(files) > 1:
                files = CLI.get_choice(
                    "Select files to upload",
                    files,
                    "Cancel",
                    allow_multiple=True
                )

            if not files:
                return

            if len(files) > 1:
                rename_action = CLI.confirm("Rename each table?", allow_cancel=True)
            if rename_action is not None:
                break

        # Begin processing files
        for file_path in files:
            file_name = os.path.basename(file_path)
            df = FileHandler.read_csv(file_path)

            table_name = CLI.get_table_name(file_name) if rename_action else FileHandler.normalize_name(file_name)
            if not table_name:
                print(f"Skipping '{file_name}'.")
                continue

            conflict_action = "replace"

            # Handle table name conflict
            while self.db.table_exists(table_name):
                action = CLI.get_choice(
                    f"Table '{table_name}' already exists. Choose an action",
                    ["Rename", "Append", "Replace"],
                    "Skip"
                )

                if not action:
                    print(f"Skipping '{file_name}'.")
                    table_name = None
                    break
                elif action == "Rename":
                    new_name = CLI.get_table_name(file_name)
                    if new_name:
                        table_name = new_name
                    continue
                elif action == "Append":
                    conflict_action = "append"
                    break
                elif action == "Replace":
                    if CLI.confirm(f"Are you sure you want to overwrite '{table_name}'?"):
                        conflict_action = "replace"
                        break

            if table_name:
                try:
                    self.db.upload_data(df, table_name, conflict_action)
                    print(f"Uploaded '{file_name}' to table '{table_name}'.")
                except Exception as e:
                    print(f"Failed to upload '{file_name}': {e}")


    def handle_download(self):
        # Let user choose tables to export as CSV
        tables = CLI.get_choice(
            "Select table(s) to download",
            self.db.get_table_names(),
            "Cancel",
            allow_multiple=True
        )
        if not tables:
            return

        same_path = False
        rename_action = True
        path = None

        if len(tables) > 1:
            same_path = CLI.confirm("Same path for each file?", allow_cancel = True)
            if same_path is None:
                return

            if same_path:
                path = CLI.get_path()
                if not path:
                    return

            rename_action = CLI.confirm("Rename each file?", allow_cancel=True)
            if rename_action is None:
                return

        for table_name in tables:
            if not same_path or not path:
                path = CLI.get_path()
                if not path:
                    print(f"Skipping '{table_name}'.")
                    continue

            file_name = FileHandler.normalize_name(table_name)

            if rename_action or not file_name:
                file_name = CLI.get_table_name(table_name)
                if not file_name:
                    print(f"Skipping '{table_name}'.")
                    continue

            full_path = os.path.join(path, f"{file_name}.csv")
            full_path = CLI.resolve_conflict_path(full_path)
            if not full_path:
                print(f"Skipping '{table_name}'.")
                continue

            df = self.db.get_table(table_name)
            FileHandler.write_csv(full_path, df)
            print(f"Downloaded '{table_name}' to '{full_path}'")

    @staticmethod
    def handle_split():
        # Let user select a CSV file to split
        files = CLI.get_files()
        if not files:
            return

        file = CLI.get_choice("Select file to split", files, "Cancel") if len(files) > 1 else files[0]
        if not file:
            return

        df = FileHandler.read_csv(file)
        print(f"\nTotal Rows: {len(df)}")
        print(f"Columns: {', '.join(df.columns)}")

        while True:
            split_action = CLI.get_choice("Choose a split method", ["Row Count", "Column Value"], "Cancel")
            if not split_action:
                return

            split_type = None
            split_value = None

            if split_action == "Row Count":
                while True:
                    raw = input("Enter row batch size (0 to cancel): ").strip()
                    try:
                        split_value = int(raw)
                        if split_value == 0:
                            break
                        if split_value > 0:
                            split_type = "row_count"
                            break
                    except ValueError:
                        print("Invalid input. Must be a positive integer.")
                if not split_type:
                    continue

            elif split_action == "Column Value":
                split_value = CLI.get_choice("Choose column to split by", df.columns.tolist(), "Cancel")
                if not split_value:
                    continue
                split_type = "column_value"

            output_path = CLI.resolve_conflict_path(CLI.get_path())
            if not output_path:
                return

            try:
                FileHandler.split(
                    file_path=file,
                    output_path=output_path,
                    split_type=split_type,
                    split_value=split_value,
                )
                print("Split completed successfully.")
                return
            except Exception as e:
                print(f"Error during split: {e}")
                return
