import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def _get_source_cred():
    return {
        "source_server": os.environ.get("SOURCE_HOST"),
        "source_database": os.environ.get("SOURCE_DB"),
        "source_username": os.environ.get("SOURCE_USER"),
        "source_password": os.environ.get("SOURCE_PASSWORD"),
    }


def _get_dest_cred():
    return {
        "dest_server": os.environ.get("DEST_HOST"),
        "dest_database": os.environ.get("DEST_DB"),
        "dest_username": os.environ.get("DEST_USER"),
        "dest_password": os.environ.get("DEST_PASSWORD"),
    }


def start_source_connection():
    source_cred = _get_source_cred()
    source_connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={source_cred['source_server']};DATABASE={source_cred['source_database']};UID={source_cred['source_username']};PWD={{{source_cred['source_password']}}}"
    source_connection = pyodbc.connect(source_connection_string)
    return source_connection


def start_dest_connection():
    dest_cred = _get_dest_cred()
    destination_connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={dest_cred['dest_server']};DATABASE={dest_cred['dest_database']};UID={dest_cred['dest_username']};PWD={{{dest_cred['dest_password']}}}"
    destination_connection = pyodbc.connect(destination_connection_string)
    destination_cursor = destination_connection.cursor()

    return destination_connection, destination_cursor


def query_sorce_db(script, connection):
    connection = start_source_connection()
    data = pd.read_sql(script, connection)
    print("data retrived from the DB")
    return data
