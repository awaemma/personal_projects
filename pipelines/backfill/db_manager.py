import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv
import time
import logging

logging.basicConfig(level= logging.INFO, format="%(asctime)s-%(levelname)s-%(message)s")


load_dotenv()

def get_mssql_db_connection(db_name):
    db_config = {
        "inward": {
            "dbname": os.environ.get("NIPINWARD_DB"),
            "user": os.environ.get("NIPINWARD_USER"),
            "password": os.environ.get("NIPINWARD_PASSWORD"),
            "host": os.environ.get("NIPINWARD_HOST")
        },
        "outward": {
             "dbname": os.environ.get("NIPOUTWARD_DB"),
            "user": os.environ.get("NIPOUTWARD_USER"),
            "password": os.environ.get("NIPOUTWARD_PASSWORD"),
            "host": os.environ.get("NIPOUTWARD_HOST"),
        }
    }
    
    logging.info(f"Fecthing {db_name} connection")
    if db_name not in db_config:
        raise ValueError(f"Database {db_name} configuration is not found.")

    config = db_config[db_name]

    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['host']};DATABASE={config['dbname']};UID={config['user']};PWD={{{config['password']}}}"
    logging.info(f"Starting.................................")
    try:    
      connection = pyodbc.connect(connection_string)
      logging.info(f"connection made to {db_name} database successfully")
    except:
        logging.info(f"Could not connect to {db_name} database. Retrying in 2 seconds")
        time.sleep(2)
        try:
          connection = pyodbc.connect(connection_string)
          logging.info(f"connection made to {db_name} database successfully")
        except Exception as error:
           print(f"Error connecting to {db_name}: {error}")

    return connection



def dest_conn():
   
   selfservice_dest = {
             "dbname": os.environ.get("DEST_DB"),
            "user": os.environ.get("DEST_USER"),
            "password": os.environ.get("DEST_PASSWORD"),
            "host": os.environ.get("DEST_HOST")
        }  
   connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={selfservice_dest['host']};DATABASE={selfservice_dest['dbname']};UID={selfservice_dest['user']};PWD={{{selfservice_dest['password']}}}"
   logging.info(f"Opening selfservice database connection")
   try:    
      connection = pyodbc.connect(connection_string)
      logging.info(f"Connection made to selfservice database successfully")
   except:
        logging.info(f"Could not connect to selfservice database. Retrying in 2 seconds")
        time.sleep(2)
        try:
          connection = pyodbc.connect(connection_string)
          logging.info(f"Connection made to selfservice database successfully")
        except Exception as error:
           print(f"Error connecting to self-service db: {error}")

   return connection

  
# connection = dest_conn()
# print(connection)
# inw_connection.close


