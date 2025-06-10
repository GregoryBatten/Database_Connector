from Scripts.cli import CLI
from Scripts.data_manager import DataManager

# Default connection settings
DEFAULT_HOST = "localhost"
DEFAULT_USER = "root"
DEFAULT_PASSWORD = "password"
DEFAULT_DATABASE = "schema"

if __name__ == "__main__":
    # Log in to the database
    db = CLI.login(DEFAULT_HOST, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_DATABASE)

    if db is not None:
        manager = DataManager(db)

        # Main menu loop
        while True:
            action = CLI.get_choice(
                f"Main Menu - {db.get_schema()}",
                ["Upload CSV", "Download CSV", "Split CSV", "Change Schema", "Create Schema"],
                "Exit"
            )

            match action:
                case "Upload CSV":
                    manager.handle_upload()

                case "Download CSV":
                    manager.handle_download()

                case "Split CSV":
                    manager.handle_split()

                case "Change Schema":
                    CLI.change_schema(db)

                case "Create Schema":
                    CLI.create_schema(db)

                case None:
                    db.close()
                    break

    print("Exiting...")
