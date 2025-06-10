import os
from Scripts.file_handler import FileHandler
from Scripts.database import Database

class CLI:
    @staticmethod
    def login(default_host, default_user, default_password, default_database):
        # Prompt for DB credentials with defaults; retry on failure
        while True:
            host = input("Enter database host: ").strip() or default_host
            user = input("Enter username: ").strip() or default_user
            password = input("Enter password: ").strip() or default_password
            database = input("Enter database name: ").strip() or default_database

            try:
                print("Please wait...")
                return Database(host, user, password, database)
            except Exception as e:
                print(f"Login failed: {e}.")
                if not CLI.confirm("Try again?"):
                    return None

    @staticmethod
    def change_schema(db: Database):
        # Let user switch to another schema
        schemas = db.get_schemas()
        if not schemas:
            print("No schemas found.")
            return

        schema = CLI.get_choice("Select Schema", schemas, "Cancel")
        if schema is not None:
            db.use_schema(schema)
            print(f"Using schema '{schema}'.")

    @staticmethod
    def create_schema(db: Database):
        # Let user create a new schema and optionally switch to it
        while True:
            name_input = input("Enter new schema name (0 to cancel): ").strip()
            if name_input == "0":
                return

            schema_name = FileHandler.normalize_name(name_input).strip().lower()
            if not schema_name:
                print("Invalid name. Please try again.")
                continue

            if db.schema_exists(schema_name):
                print(f"Schema '{schema_name}' already exists.")
                continue

            try:
                db.create_schema(schema_name)
                print(f"Schema '{schema_name}' created.")
                if CLI.confirm("Use new schema?"):
                    db.use_schema(schema_name)
                    print(f"Using schema '{schema_name}'.")
                return
            except ValueError as e:
                print(f"Error: {e}")

    @staticmethod
    def get_choice(title, options, back_option, allow_multiple=False):
        # Prompt user to select from a list (supports multiple if allowed)
        while True:
            print(f"\n{title}:")
            for i, option in enumerate(options, 1):
                print(f"{i}. {option}")
            if allow_multiple:
                print(f"{len(options) + 1}. All")
            print(f"0. {back_option}")

            raw_input = input("Select: ").strip()
            if not raw_input:
                print("No selection made.")
                continue

            try:
                selected = [int(i.strip()) for i in raw_input.split(",")]
            except ValueError:
                print("Enter numbers only.")
                continue

            if any(i < 0 or i > len(options) + allow_multiple for i in selected):
                print("Selection out of range.")
                continue

            if not allow_multiple and len(selected) > 1:
                print("Select only one option.")
                continue

            if allow_multiple and len(selected) > 1 and set(selected) & {0, len(options) + 1}:
                print("Cannot mix 'All' or 'Cancel' with others.")
                continue

            if selected == [0]:
                return None

            if allow_multiple:
                if selected == [len(options) + 1]:
                    return options
                return [options[i - 1] for i in selected]

            return options[selected[0] - 1]

    @staticmethod
    def confirm(prompt, allow_cancel=False):
        # Prompt for yes/no confirmation
        while True:
            answer = input(f"{prompt} (y/n, 0 to cancel): ").strip().lower() if allow_cancel else input(f"{prompt} (y/n): ").strip().lower()
            if allow_cancel and answer == "0":
                return None
            if answer in ['y', 'yes']:
                return True
            elif answer in ['n', 'no']:
                return False

    @staticmethod
    def get_path():
        # Prompt for a valid file or folder path
        while True:
            path = input("Enter file/folder path (0 to cancel): ").strip()
            if not path:
                print("Path cannot be empty.")
                continue
            if path == "0":
                return None
            if os.path.isdir(path) or os.path.isfile(path):
                return path

    @staticmethod
    def get_files(target=".csv"):
        # Let user select folder and get matching files
        while True:
            path = CLI.get_path()
            if path is None:
                return None

            files = FileHandler.get_files(path, target)
            if files:
                return files
            print("No files found.")

    @staticmethod
    def get_table_name(file_name, fallback_name="default_table"):
        # Prompt for a table name; allow skip
        while True:
            default_name = FileHandler.normalize_name(file_name) or FileHandler.normalize_name(fallback_name)
            table_name = input(f"Enter table name (default: {default_name}, 0 to skip): ").strip() or default_name
            if table_name == "0":
                return None
            
            table_name = FileHandler.normalize_name(table_name)
            if not table_name:
                print("Invalid name.")
                continue
            
            return table_name

    @staticmethod
    def resolve_conflict_path(initial_path):
        # Resolve conflict if file already exists
        while FileHandler.file_exists(initial_path):
            conflict_action = CLI.get_choice(
                f"File '{initial_path}' already exists. Choose an action",
                ["Rename", "Replace", "Change Path"],
                "Skip"
            )

            match conflict_action:
                case "Rename":
                    new_name = CLI.get_table_name(os.path.basename(initial_path))
                    if not new_name:
                        continue
                    initial_path = os.path.join(os.path.dirname(initial_path), f"{new_name}.csv")

                case "Replace":
                    return initial_path

                case "Change Path":
                    new_path = CLI.get_path()
                    if not new_path:
                        continue
                    initial_path = os.path.join(new_path, os.path.basename(initial_path))

                case None:
                    return None

        return initial_path
