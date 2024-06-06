
from datetime import datetime
from db_manager import start_source_connection, start_dest_connection, query_sorce_db


sys_path = "D:/Pipelines/account_opening/"
conv_acct_script = sys_path + "scripts/conventional_account_opening.sql"

etl_starttime = datetime.now()
print(f"Conventional ETL started at at - {etl_starttime}")


with open(conv_acct_script, "r") as file:
    conv_acct_script = file.read()


source_connection = start_source_connection()
destination_connection, destination_cursor = start_dest_connection()

data = query_sorce_db(conv_acct_script, source_connection)


try:
    destination_cursor.execute("Truncate table AccountOpening_Fact_tbl")

    destination_connection.commit()

except Exception as e:
    print(f"table could not be truncated: {str(e)}")

data_type_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "NVARCHAR(100)",
    "datetime64[ns]": "DATETIME", 
}

# checking for exisiting table or proceed to create one with the datatypes of the incoming df
try:
    # Check if the table already exists
    table_name = "AccountOpening_Fact_tbl"  # Table name
    if not destination_cursor.tables(table=table_name, tableType="TABLE").fetchone():
        # Table doesn't exist, so create it with an "insert_date" column
        columns = ", ".join(
            [f"{col} {data_type_mapping[data[col].dtype.name]}" for col in data.columns]
        )
        table_create_sql = (
            f"CREATE TABLE {table_name} (inserted_date DATETIME, {columns})"
        )
        destination_cursor.execute(table_create_sql)
        destination_connection.commit()

except Exception as e:
    destination_connection.rollback()
    print(f"table creation/check operation rolled back: {str(e)}")


# this would be used for the insert_datetime
current_datetime = datetime.now()

print(f" insert start time is {current_datetime}")

# inserting data into the mssql table
try:
    destination_connection.autocommit = False

    # Prepare the insert SQL statement
    insert_sql = f"INSERT INTO {table_name} (inserted_date, {', '.join(data.columns)}) VALUES (?, {', '.join(['?']*len(data.columns))})"

    rows_to_insert = [
        (
            current_datetime,
            id,
            date,
            status,
            description,
            CustomerType,
            Customer_Status,
            product_desc,
            branch,
            Region,
            Zone,
            State_Located,
            Account_Status,
            channel,
        )
        for id, date, status, description, CustomerType, Customer_Status, product_desc, branch, Region, Zone, State_Located, Account_Status, channel in zip(
            data["id"],
            data["date"],
            data["status"],
            data["description"],
            data["CustomerType"],
            data["Customer_Status"],
            data["product_desc"],
            data["branch"],
            data["Region"],
            data["Zone"],
            data["State_Located"],
            data["Account_Status"],
            data["channel"],
        )
    ]

    if len(rows_to_insert) > 0:
        destination_cursor.executemany(insert_sql, rows_to_insert)
        destination_connection.commit()

        print("inserted successfully")
        now = datetime.now()

        print(f"inserted endtime at - {now}")


except Exception as e:
    destination_connection.rollback()
    print(f"Insert operation rolled back: {str(e)}")
finally:
    destination_connection.autocommit = True 
    print("ETL Completed")


source_connection.close()
destination_cursor.close()
destination_connection.close()

etl_endtime = datetime.now()
print(f" Conventional ETL completed at at - {etl_endtime}")
