from db_manager import get_mssql_db_connection, dest_conn
from column_map import column_mapper
import pandas as pd
import numpy as np
import sys
import logging

logging.basicConfig(level= logging.INFO, format="%(asctime)s-%(levelname)s-%(message)s")

def _get_script(script):
    script_path =  "D:/SelfService_ETL/backfill/script/"
    script_path = script_path + script
    logging.info(f"Fecthing the sql script at {script_path}")
    with open(script_path, 'r') as script_file:
        script = script_file.read()
    
    return script

def fetch_mssql_data(sql_script,db_name):
    script = _get_script(sql_script)
    connection = get_mssql_db_connection(db_name)
    data = pd.read_sql(script, connection)
    logging.info(f"Fecthing the data from the {db_name} database.... hold on")
    connection.close()
    
    return data

column_mapping = column_mapper()


def main():
    if len(sys.argv) != 3:
        print("Usage: python main.py <script.sql> <conn>")
        sys.exit(1)

    sql_script = sys.argv[1]
    db_name = sys.argv[2]
    data = fetch_mssql_data(sql_script, db_name)
    
    return data


if __name__ == "__main__":
    data = main()
    # Create a list of columns present both in the DataFrame and the mapping
    columns_to_insert = [col for col in column_mapping.keys() if col in data.columns]
    data = data.astype(str) 
    data = data.replace('nan', np.nan)
    # Convert np.nan to None for proper NULL insertion
    data = data.where(pd.notnull(data), None)

    # initializing destination connection.
    dest_connection = dest_conn()
    dest_cursor = dest_connection.cursor()

    # Below is no longer needed since all the columns are converted to string for easy insertion.
    # Just leaving it for now...
    data_type_mapping = {
    "int64": "NVARCHAR(100)",
    "float64": "NVARCHAR(100)",
    "object": "NVARCHAR(100)",
    "datetime64[ns]": "NVARCHAR(100)",
        }
    # checking for exisiting table or proceed to create one with the datatypes of the incoming df
    # Below table creation is not needed too. This will be re-factored in a future version. 
    try:
        # Check if the table already exists
        table_name = "Fact_tbl_backfill"  # Table name
        if not dest_cursor.tables(table=table_name, tableType="TABLE").fetchone():
            # Table doesn't exist, so create it with an "insert_date" column
            columns = ", ".join(
                [f"{col} {data_type_mapping[data[col].dtype.name]}" for col in data.columns]
            )
            table_create_sql = (
                f"CREATE TABLE {table_name} (inserted_date DATETIME, {columns})"
            )
            dest_cursor.execute(table_create_sql)
            dest_connection.commit()

    except Exception as e:
        dest_connection.rollback()
        print(f"table creation/check operation rolled back: {str(e)}")
        
   
    # Generate the SQL query dynamically
    logging.info(f"Generating dynamic sql for data insertion into {table_name}")
    columns_str = ', '.join(column_mapping[col] for col in columns_to_insert)
    placeholders = ', '.join('?' for _ in columns_to_insert)
    sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    

    data_to_insert = data[columns_to_insert].values.tolist()
    # Using executemany to batch insert the data
    logging.info(f"Data insertion has started.... please hold")
    dest_cursor.executemany(sql, data_to_insert)
    logging.info(f"Data insertion has been concluded successfully")
    # Commiting the transaction
    dest_connection.commit()
    # Closing the cursor and connection
    dest_cursor.close()
    dest_connection.close()
    logging.info(f"Closing connection")


