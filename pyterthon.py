#insert storageDrive.csv to Microsoft Sql Server database

import csv
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib

df = pd.read_csv('walmart.csv')
print(pyodbc.drivers())

import urllib

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:nextchsolutions.database.windows.net,1433;DATABASE=NexTechSolutions;UID=ktfs;PWD=Password1!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params,fast_executemany=True)

#Put the 'raw data' into a SQL database
df.to_sql('WalmartDb', engine, if_exists='append', index=False)
