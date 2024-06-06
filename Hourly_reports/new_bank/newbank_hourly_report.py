


import pandas as pd
import psycopg2
import numpy as np
from datetime import date, datetime, timedelta
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pretty_html_table import build_table


timestamp = datetime.now().strftime("%Y-%m-%d %I:%M:%S %p")


#declaring system path
sys_path = 'D:/Hourly_reports/newbank/'
#declaing SQL script path
folder_path = sys_path+'script'

#accessing DB credentials
f_open = open(sys_path + 'configurations.json')
config = json.load(f_open)
f_open.close()

#setting db connections for newbank_transfer DB
db_params_transfer = {
    "host": config['DB_connection']['server'],
    "database": config['DB_connection']['database'],
    "user": config['DB_connection']['username'],
    "password": config['DB_connection']['password'],
    "port": config['DB_connection']['port']
}

#setting db connections for newbank_billspayment DB
db_params_billspayment = {
    "host": config['DB_connection']['server'],
    "database": config['DB_connection']['database_billspayment'],
    "user": config['DB_connection']['username'],
    "password": config['DB_connection']['password'],
    "port": config['DB_connection']['port']
}

#Email configuration
smtp_server = config['SMTP_config']['smtp_server']
smtp_port = config['SMTP_config']['smtp_port']
recipient_email = config['SMTP_config']['recipient_email'] 
cc_email = config['SMTP_config']['cc_email'] 
sender_email = config['SMTP_config']['sender_email']


try:
    # Establishing a connection to the database
    transfer_connection = psycopg2.connect(**db_params_transfer)
    # Creating a cursor object to interact with the database
    transfer_cursor = transfer_connection.cursor()
    # Performing database operations here...
except (Exception, psycopg2.Error) as error:
    print(f"Error connecting to the transfer_database: {error}")
    
try:
    # Establishing a connection to the database
    billspayment_connection = psycopg2.connect(**db_params_billspayment)
    # Creating a cursor object to interact with the database
    billspayment_cursor = billspayment_connection.cursor()
    # Performing database operations here...
except (Exception, psycopg2.Error) as error:
    print(f"Error connecting to the billspayment_database: {error}")
    
# extracting all the scrip path and updating same into a dictionary
all_scripts_path = {}
for file_name in os.listdir(folder_path):
#     print(file_name.split('.')[0])
        script_path = os.path.join(folder_path, file_name)
        # Check if the file is a SQL script
        if os.path.isfile(script_path) and script_path.endswith('.sql'):
            with open(script_path, 'r') as script_file:
                    script = script_file.read()
                    all_scripts_path.update({file_name.split('.')[0]:script})

 # running all the script and saving the result as a pandas dataframe                   
all_data = {}
for key, value in all_scripts_path.items():
  # the postgres DB does not allow cross database referencing 
  # hence different cursor objects has to be used for Billspayment DB and Transfer DB.
    if key in ('Airtime_Summary','BillsPayment_Summary'):
        billspayment_cursor.execute(value)
        result = billspayment_cursor.fetchall()
        data = pd.DataFrame(result, columns=[desc[0] for desc in billspayment_cursor.description])
        all_data.update({key:data})
       
    if key not in ('Airtime_Summary','BillsPayment_Summary'):
        transfer_cursor.execute(value)
        result = transfer_cursor.fetchall()
        data = pd.DataFrame(result, columns=[desc[0] for desc in transfer_cursor.description])
        all_data.update({key:data})  
       
    
#extacting the pandas df result
account_statement = all_data["Account_Statment_Delivery"]
airtime = all_data["Airtime_Summary"]
bills_payment = all_data["BillsPayment_Summary"]
interbank = all_data["InterBank_Transaction_Summary"]
intrabank = all_data["IntraBank_Transaction_Summary"]
newbank_intra = all_data["newbank_IntraBank_Transaction_Summary"]

#closing connection
transfer_connection.close()
transfer_cursor.close()
billspayment_connection.close()
billspayment_cursor.close()


# applying the pretty_html_table package to make the tables visually appealing
interbank_output = build_table(interbank, 'blue_dark',font_size='12px',font_family='Open Sans',
                               width_dict=['auto','250px', 'auto', 'auto','150px', '150px'],padding='1px')
intrabank_output = build_table(intrabank, 'blue_dark',font_size='12px',font_family='Open Sans',
                               width_dict=['50px','300px', 'auto', 'auto','150px', '150px'],padding='1px')
newbank_intra_output = build_table(newbank_intra, 'blue_dark',font_size='12px',font_family='Open Sans',
                                    width_dict=['auto','250px', 'auto', 'auto','150px', '150px'],padding='1px')
account_statement_output = build_table(account_statement, 'blue_dark',font_size='12px',font_family='Open Sans',
                                       width_dict=['auto','250px', 'auto', 'auto','180px', '180px'],padding='1px')
airtime_output = build_table(airtime, 'blue_dark',font_size='12px',font_family='Open Sans',
                              width_dict=['100px','250px', 'auto', 'auto','150px', '150px'],padding='1px')
bills_payment_output = build_table(bills_payment, 'blue_dark',font_size='12px',font_family='Open Sans',
                                   width_dict=['100px','250px', 'auto', 'auto','150px', '150px'],padding='1px')


# Create the email message
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = recipient_email
# msg['Cc'] = cc_email
msg['Cc'] = ', '.join(cc_email)

msg['Subject'] = f'newbank App Hourly Report as at {timestamp}'
salute = 'Dear Team,'
message = f"Please see below newbank app hourly report as at {timestamp}:"

#body of the email
email_body = f'<p>{salute}</p>{message}<br><br><br><caption><u><b>INTERBANK TRANSFERS</b></caption>{interbank_output}</table>'\
             f'<br><br><caption><u><b>bigbank-bigbank INTRABANK TRANSFERS</b></u></caption>{intrabank_output}</table>'\
             f'<br><br><caption><u><b>bigbank-newbank INTRABANK TRANSFERS</b></u></caption>{newbank_intra_output}</table>'\
             f'<br><br><caption><u><b>ACCOUNT STATEMENT DELIVERY</b></u></caption>{account_statement_output}</table>'\
             f'<br><br><caption><u><b>AIRTIME</b></u></caption>{airtime_output}</table>'\
             f'<br><br><caption><u><b>BILLS PAYMENT</b></u></caption>{bills_payment_output}</table>'\

             
msg.attach(MIMEText(email_body, 'html'))   


# Establish an SMTP connection and send the email
try:
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    #server.login(smtp_username, smtp_password)
    recipients = [recipient_email] + cc_email  # Include both To and CC recipients
    server.sendmail(sender_email, recipients, msg.as_string())
    server.quit()
    print("Email sent successfully")
except Exception as e:
    print("Error: Unable to send email -", str(e))



