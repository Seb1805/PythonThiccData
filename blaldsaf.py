import pandas as pd
import pyodbc
import sqlalchemy
        
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:nextchsolutions.database.windows.net,1433;DATABASE=NexTechSolutions;UID=ktfs;PWD=Password1!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
conn = pyodbc.connect(conn_str)

# Define the SQL query
query = '''
SELECT * FROM WalmartDb
'''

# Execute the query and fetch the results into a pandas DataFrame
df = pd.read_sql(query, conn)

# Close the database connection
conn.close()

# Print the DataFrame
print(df)