import pyodbc
import csv

# Connect to MSSQL database
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password')

# Create a cursor object
cursor = conn.cursor()

# Read CSV file and update records based on i_wts_key
with open('your_csv_file.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        i_wts_key = row['i_wts_key']
        trade_owner = row.get('trade_owner')
        account_owner = row.get('account_owner')
        fx_owner = row.get('fx_owner')

        # Update specific column records in MSSQL table based on i_wts_key
        if trade_owner is not None:
            cursor.execute("UPDATE your_table_name SET trade_owner=? WHERE i_wts_key=?", (trade_owner, i_wts_key))
        if account_owner is not None:
            cursor.execute("UPDATE your_table_name SET account_owner=? WHERE i_wts_key=?", (account_owner, i_wts_key))
        if fx_owner is not None:
            cursor.execute("UPDATE your_table_name SET fx_owner=? WHERE i_wts_key=?", (fx_owner, i_wts_key))
        conn.commit()

# Close connection
conn.close()
