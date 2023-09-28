import luigi
import pyodbc
import pandas as pd
import csv

class ExtractDataTask(luigi.Task):
    def run(self):
        # Set up the connection to the SQL Server database
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:nextchsolutions.database.windows.net,1433;DATABASE=NexTechSolutions;UID=ktfs;PWD=Password1!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
        conn = pyodbc.connect(conn_str)
        print("Extracting the data..")
        # Define the SQL query
        query = '''
            SELECT * FROM WalmartDb
        '''

        # Execute the query and fetch the results into a pandas DataFrame
        df = pd.read_sql(query, conn)
        df.to_csv('walmart_extracted.csv', index=False)
        # Close the database connection
        conn.close()

    def output(self):
        return luigi.LocalTarget('walmart_extracted.csv')


class TransformDataTask(luigi.Task):
    def requires(self):
        return ExtractDataTask()

    def run(self):
        input_file = self.input().path
        df = pd.read_csv(input_file)
        df = df.drop_duplicates()
        df = df.dropna()
        df.to_csv('walmart_transformed.csv', index=False)

    def output(self):
        return luigi.LocalTarget('walmart_transformed.csv')


class LoadDataTask(luigi.Task):
    def requires(self):
        return TransformDataTask()

    def run(self):
        input_file = self.input().path
        with open(input_file, 'r') as file:
            data = [row for row in csv.reader(file)]

        with open('walmart_clean.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)

    def output(self):
        return luigi.LocalTarget('walmart_clean.csv')


class MyPipeline(luigi.WrapperTask):
    def requires(self):
        return LoadDataTask()


if __name__ == '__main__':
    luigi.run(main_task_cls=MyPipeline, local_scheduler=True)
