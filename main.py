import os
import psycopg2
import io
import sys
from pandas import DataFrame, read_csv
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq

#bigquery credentials and destinations

credentials = service_account.Credentials.from_service_account_info(
        {},
    )


#glcoud project name
project = 'yourprojectname' 

#add tables or views from the postgre env you want to export
tables_export = ['tablename'] 
for a in tables_export:
    GO = 1
    # Check if the file path exists.
    if GO > 0:
        try:
            # Connect to database. update the login details accordingly..
            connect = psycopg2.connect(host='hostname.host.com', database='db-name',
                                        user='rusername', password='apassword')
            print ("Connected to db..")
        except psycopg2.DatabaseError as e:
            # Confirm unsuccessful connection and stop program execution.
            print("Database connection unsuccessful.")
            quit()
        # Cursor to execute query.
        cursor = connect.cursor()
        # SQL to select data from the person table.
        sqlSelect = \
            'SELECT *\
             FROM public.' + (a) + ""
        try:
            print('Executing ' + (a) + ' query (this may take a while)..')
            df2 = pd.concat([chunk for chunk in pd.read_sql(sqlSelect,connect,chunksize=1000)])
            df2.index.name = 'pandasindex'
            df2.info(memory_usage="deep")
            print('generating preview of ' + (a) + '..')
            print(df2)
            # Message stating export successful.
            print("Data export successful.")
            print('Attempting upload to bigquery of ' + (a) + '..')
            destination_table = 'destination.' + (a)# + '_test'
            df2.to_gbq(destination_table, project, if_exists='replace', credentials=credentials)
            print("Data export unsuccessful.")      
        except psycopg2.DatabaseError as e:
            # Message stating export unsuccessful.
            print("Data export unsuccessful.")
            quit()
        finally:
            # Close database connection.
            connect.close()
    else:
        # Message stating file path does not exist.
        print("File path does not exist.") 
