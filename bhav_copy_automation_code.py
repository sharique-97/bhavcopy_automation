#!/usr/bin/env python
# coding: utf-8

# In[1]:


import webbrowser
import time
import os
import shutil
import pandas as pd
import mysql.connector

chrome_path = "C:/Program Files/Google/Chrome/Application/chrome.exe"

# Create a "equity_may_2023" folder in the current working directory if not present
folder_name = "equity_may_2023"
folder_path = os.path.join(os.getcwd(), folder_name)
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Iterate over the days of May 2023
for day in range(1, 32):
    # Format the day to have leading zeros if needed
    formatted_day = str(day).zfill(2)

    # Construct the link for the specific day
    link = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{formatted_day}0523_CSV.ZIP"

    try:
        # Open the link in Chrome
        webbrowser.register('chrome', None, webbrowser.BackgroundBrowser(chrome_path))
        webbrowser.get('chrome').open(link)

        # Wait for the file to be downloaded
        time.sleep(5)  # Adjust the sleep duration as needed

        # Get the downloaded file's name
        file_name = link.split("/")[-1]

        # Move the downloaded file to the "equity_may_2023" folder, replacing it if it already exists
        download_path = os.path.join(os.path.expanduser("~"), "Downloads", file_name)
        destination_path = os.path.join(folder_path, file_name)
        shutil.move(download_path, destination_path, copy_function=shutil.copy2)

        print(f"File '{file_name}' downloaded to the '{folder_name}' folder.")

    except Exception as e:
        print(f"Error occurred for link {link}: {str(e)}")


# Close the browser after the last file
os.system("taskkill /im chrome.exe /f")


# In[5]:


# Connect to the MySQL server
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database =  'equity_may_2023')

# Create a cursor object to execute queries
cursor = connection.cursor()

create_table_query = """
CREATE TABLE equity (
    SC_CODE INT,
    SC_NAME VARCHAR(255),
    SC_GROUP VARCHAR(255),
    SC_TYPE VARCHAR(255),
    OPEN DECIMAL(10, 2),
    HIGH DECIMAL(10, 2),
    LOW DECIMAL(10, 2),
    CLOSE DECIMAL(10, 2),
    LAST DECIMAL(10, 2),
    PREVCLOSE DECIMAL(10, 2),
    NO_TRADES INT,
    NO_OF_SHRS INT,
    NET_TURNOV DECIMAL(18, 2),
    TDCLOINDI VARCHAR(255)
)
"""

cursor.execute(create_table_query)
connection.commit()

# Execute the query to show tables in the selected database
cursor.execute("SHOW TABLES")

# Fetch all the table names
table_names = cursor.fetchall()

# Print the table names
for table_name in table_names:
    print(table_name[0])


# In[6]:


# Changing dir to equity folder

os.chdir('equity_may_2023')
directory = os.getcwd()

# Initialize an empty list to store the DataFrames
dfs = []

# Iterate through all the files in the directory
for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        
        # Read the CSV file as a DataFrame
        df = pd.read_csv(file_path)
        print(df.head())
        # Append the DataFrame to the list
        dfs.append(df)


# Inserting dfs into the table
from sqlalchemy import create_engine

connection = create_engine('mysql+mysqlconnector://root:1234@localhost/equity_may_2023')

for df in dfs:
    df.to_sql(name='equity', con=connection, if_exists='append', index=False)
connection.dispose()


# In[7]:


connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database =  'equity_may_2023')

# Create a cursor object to execute queries
cursor = connection.cursor()

query = """
SELECT SC_CODE, AVG(OPEN) AS AVG_OPEN, AVG(HIGH) AS AVG_HIGH, AVG(LOW) AS AVG_LOW, AVG(CLOSE) AS AVG_CLOSE
FROM equity
GROUP BY SC_CODE
"""

cursor.execute(query)

# Fetch all the rows of the result
result = cursor.fetchall()

# Print the result
for row in result:
    sc_code = row[0]
    avg_open = row[1]
    avg_high = row[2]
    avg_low = row[3]
    avg_close = row[4]
    print(f"Stock: {sc_code}, Average Open: {avg_open}, Average High: {avg_high}, Average Low: {avg_low}, Average Close: {avg_close}")

# Close the cursor and connection
cursor.close()
connection.close()



