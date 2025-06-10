from typing import Literal
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

class Database:
    def __init__(self, host, user, password, database):
        # Initialize connection and cursor
        self.host = host
        self.user = user
        self.password = password
        self._schema = database

        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            use_unicode=True
        )
        self.cursor = self.conn.cursor()

    def get_schema(self):
        # Return the current schema name
        return self._schema

    def use_schema(self, db_name):
        # Switch to a different schema
        self.cursor.execute(f"USE `{db_name}`")
        self._schema = db_name

    def get_schemas(self):
        # Return list of all schemas in the database
        self.cursor.execute("SHOW DATABASES")
        rows = self.cursor.fetchall()
        return [row[0] for row in rows if isinstance(row, tuple) and len(row) > 0]

    def schema_exists(self, schema_name):
        # Check if a specific schema exists
        self.cursor.execute("SHOW DATABASES LIKE %s", (schema_name,))
        return self.cursor.fetchone() is not None

    def create_schema(self, schema_name):
        # Create a new schema if it doesn't already exist
        schema_name = schema_name.strip().lower()
        if not schema_name:
            raise ValueError("Schema name cannot be empty.")
        if self.schema_exists(schema_name):
            raise ValueError(f"Schema '{schema_name}' already exists.")

        self.cursor.execute(f"CREATE DATABASE `{schema_name}`")
        self.conn.commit()

    def get_table_names(self):
        # Return list of all tables in the current schema
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()
        return [table[0] for table in tables if isinstance(table, tuple) and len(table) > 0]

    def table_exists(self, table_name):
        # Check if a specific table exists in the current schema
        self.cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        return self.cursor.fetchone() is not None

    def upload_data(self, df: pd.DataFrame, table_name: str, mode: Literal["replace", "append", "fail"] = "replace"):
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self._schema}"
        )
        df.to_sql(name=table_name, con=engine, index=False, if_exists=mode)


    def get_table(self, table_name: str) -> pd.DataFrame:
        # Retrieve entire table as a DataFrame
        query = f"SELECT * FROM `{table_name}`"
        engine = create_engine(
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self._schema}"
        )
        return pd.read_sql(query, con=engine)

    def close(self):
        # Cleanly close the connection and cursor
        self.cursor.close()
        self.conn.close()
