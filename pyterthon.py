#insert storageDrive.csv to Microsoft Sql Server database

import csv
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib

#connect to database
# conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:nextchsolutions.database.windows.net,1433;Database=NexTechSolutions;Uid=ktfs;Pwd=Password1!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;')

# engine = sqlalchemy.create_engine(
#                "mssql+pyodbc://ktfs:Password1!@nextchsolutions.database.windows.net/NexTechSolutions",
#                echo=False)

# cursor = conn.cursor()

df = pd.read_csv('walmart.csv')
print(pyodbc.drivers())

import urllib

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:nextchsolutions.database.windows.net,1433;DATABASE=NexTechSolutions;UID=ktfs;PWD=Password1!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)

df.to_sql('WalmartDb', engine, if_exists='append', index=False)

#insert data to database
#df.to_sql('WalmartDb', engine, if_exists='replace', index=False)