
from datetime import datetime
from db_manager import start_source_connection, start_dest_connection, query_sorce_db


sys_path = "D:/Pipelines/account_opening/"
conv_acct_script = sys_path + "scripts/TAB_restriction_table.sql"

etl_starttime = datetime.now()
print(f"TAB restriction ETL started at at - {etl_starttime}")


with open(conv_acct_script, "r") as file:
    conv_acct_script = file.read()


source_connection = start_source_connection()
destination_connection, destination_cursor = start_dest_connection()

data = query_sorce_db(conv_acct_script, source_connection)
data.fillna("No Value", inplace=True)
data.replace("No Value", None, inplace=True)

# data['REASON_CODE'] = data['REASON_CODE'].astype(str)
data["rn"] = data["rn"].astype(str)


try:
    destination_cursor.execute("Truncate table TAB_restriction_tbl")

    destination_connection.commit()

except Exception as e:
    print(f"table could not be truncated: {str(e)}")

data_type_mapping = {
    "int64": "NVARCHAR(MAX)",
    "float64": "NVARCHAR(MAX)",
    "object": "NVARCHAR(MAX)",
    "datetime64[ns]": "DATETIME",  # Adjust this for string types
}

# checking for exisiting table or proceed to create one with the datatypes of the incoming df
try:
    # Check if the table already exists
    table_name = "TAB_restriction_tbl"  # Table name
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
            additional_reference,
            comment,
            status,
            code
        )
        for additional_reference, comment, status, code,  in zip(
            data["additional_reference"],
            data["comment"],
            data["status"],
            data["code"]
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
    destination_connection.autocommit = True  # Restore autocommit mode
    print("TAB restriction ETL Completed")


source_connection.close()
destination_cursor.close()
destination_connection.close()

etl_endtime = datetime.now()
print(f"TAB restriction ETL completed at at - {etl_endtime}")

