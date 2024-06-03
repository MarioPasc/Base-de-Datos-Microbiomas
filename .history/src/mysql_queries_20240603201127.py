import os
import time
import mysql.connector as mysql
import pandas as pd

class Queries:
    def __init__(self, password: str, database: str) -> None:
        self.password = password
        self.database = database

    def __connection(self):
        """
        Establishes a connection to the MySQL database.
        Returns:
            connection: A MySQL connection object.
        """
        try:
            connection = mysql.connect(
                host="localhost",
                user="root",
                password=self.password,
                database=self.database
            )
            print("Database connection established")
            return connection
        except mysql.Error as err:
            print(f"Error: {err}")
            return None

    def __query_format(self, query: str, argument: tuple, num_qry: int):
        """
        Executes a given query and saves the results to a CSV file.
        
        Args:
            query (str): The SQL query to execute.
            argument (tuple): Arguments for the SQL query.
            num_qry (int): Query number used for naming the output file.
            
        Returns:
            final_time (float): Time taken to execute the query.
        """
        connection = self.__connection()
        if connection is None:
            return
        
        cursor = connection.cursor()
        try:
            start_time = time.time()
            cursor.execute(query, argument)
            result = cursor.fetchall()
            final_time = time.time() - start_time
            col_name = [d[0] for d in cursor.description]

            # Debugging: Print the result and column names
            print(f"Query {num_qry} executed in {final_time:.4f} seconds.")
            print("Column names:", col_name)
            print("Number of rows returned:", len(result))

            if result:
                self.__export_csv(result, col_name, f"query{num_qry}.csv")
            else:
                print(f"Query {num_qry} returned no results.")
            return final_time
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def query1(self):
        query = '''SELECT p.Patient_ID, COUNT(DISTINCT sm.Microorganism_ID) AS Num_Microorganisms
                   FROM patient p
                   JOIN sample s ON p.Patient_ID = s.Patient_ID
                   JOIN sample_microorganism sm ON s.Sample_ID = sm.Sample_ID
                   GROUP BY p.Patient_ID
                   ORDER BY Num_Microorganisms DESC
                   LIMIT 10;'''
        return self.__query_format(query, (), 1)

    def query2(self, microorganism_ID: str):
        query = '''SELECT sm.Sample_ID, sm.qPCR
                   FROM sample_microorganism sm
                   WHERE sm.Microorganism_ID = %s 
                   ORDER BY sm.qPCR DESC;'''
        return self.__query_format(query, (microorganism_ID,), 2)

    def query3(self, disease: str):
        query = '''SELECT p.Patient_ID, s.Sample_ID, s.Date, s.Body_Part, s.Sample_Type
                   FROM patient p
                   JOIN sample s ON p.Patient_ID = s.Patient_ID
                   WHERE p.Disease = %s
                   ORDER BY s.Date;'''
        return self.__query_format(query, (disease,), 3)

    def query4(self):
        query = '''SELECT s.Sample_Type, COUNT(*) AS Sample_Count
                   FROM sample s
                   GROUP BY s.Sample_Type
                   ORDER BY Sample_Count DESC;'''
        return self.__query_format(query, (), 4)
    
    def query5(self):
        query = '''SELECT sm.Microorganism_ID, s.Sample_Type, COUNT(sm.Sample_ID) AS Sample_Count, AVG(sm.qPCR) AS avg_qPCR, STDDEV(sm.qPCR) AS stddev_qPCR
                   FROM sample s
                   JOIN sample_microorganism sm ON s.Sample_ID = sm.Sample_ID
                   GROUP BY s.Sample_Type, sm.Microorganism_ID
                   ORDER BY sm.Microorganism_ID DESC;'''
        return self.__query_format(query, (), 5)

    def query6(self):
        query = '''SELECT p1.Patient_ID, s1.Max_qPCR
                   FROM (SELECT p.Patient_ID
                         FROM patient p
                         WHERE p.Disease='Hepatitis B') p1
                   JOIN (SELECT s.Patient_ID, MAX(sm.qPCR) AS Max_qPCR
                         FROM sample s
                         JOIN sample_microorganism sm ON s.Sample_ID = sm.Sample_ID
                         JOIN microorganism m ON sm.Microorganism_ID = m.Microorganism_ID
                         WHERE m.Species='Hepatitis B Virus'
                         GROUP BY s.Patient_ID) s1
                   ON s1.Patient_ID = p1.Patient_ID;'''
        return self.__query_format(query, (), 6)

    def query7(self):
        query = '''SELECT Species, COUNT(*) AS Count, AVG(Seq_length) AS avg_SeqLength
                   FROM microorganism
                   GROUP BY Species
                   HAVING COUNT(*) > 1;'''
        return self.__query_format(query, (), 7)

    def __export_csv(self, result, col_name, file_name: str):
        """
        Exports the query results to a CSV file.
        
        Args:
            result: The result set of the query.
            col_name: Column names of the result set.
            file_name (str): Name of the CSV file.
        """
        file_path = os.path.join(os.getcwd(), "csv-queries", file_name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df = pd.DataFrame.from_records(result, columns=col_name)
        df.to_csv(file_path, index=False)
        print(f"CSV file {file_name} has been created successfully with {len(result)} records.")
