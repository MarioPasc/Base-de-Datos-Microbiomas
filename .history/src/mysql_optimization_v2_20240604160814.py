import mysql.connector
import time
import itertools
import pandas as pd
import numpy as np
from tqdm import tqdm

class QueryOptimizer:
    def __init__(self, password, database):
        self.password = password
        self.database = database

    def connect_to_db(self):
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password=self.password,
            database=self.database
        )

    def execute_query(self, query, params=None):
        """Executes a given SQL query and returns the duration of the execution."""
        connection = self.connect_to_db()
        cursor = connection.cursor()
        try:
            start_time = time.time()
            cursor.execute(query, params)
            cursor.fetchall()  # Fetch results to complete query execution
            duration = time.time() - start_time
        except mysql.connector.Error as err:
            print(f"Error executing query: {err}")
            duration = np.nan
        finally:
            cursor.close()
            connection.close()
        return duration

    def drop_index(self, index_name, table_name):
        """Drops an index from a specified table if it exists."""
        connection = self.connect_to_db()
        cursor = connection.cursor()
        try:
            cursor.execute(f"SHOW INDEX FROM {table_name} WHERE Key_name='{index_name}';")
            result = cursor.fetchone()
            if result:
                cursor.execute(f"ALTER TABLE {table_name} DROP INDEX {index_name};")
        except mysql.connector.Error as err:
            print(f"Error dropping index {index_name} on {table_name}: {err}")
        finally:
            cursor.close()
            connection.close()

    def drop_indices(self):
        """Drops all specified indices."""
        indices_info = [
            ("idx_patient_id", "patient"),
            ("idx_sample_patient_id", "sample"),
            ("idx_sample_id", "sample"),
            ("idx_sample_microorganism_sample_id", "sample_microorganism"),
            ("idx_sample_microorganism_microorganism_id", "sample_microorganism"),
            ("idx_sample_microorganism_qpcr", "sample_microorganism"),
            ("idx_patient_disease", "patient"),
            ("idx_sample_date", "sample"),
            ("idx_sample_type", "sample"),
            ("idx_microorganism_species", "microorganism")
        ]
        for index_name, table_name in indices_info:
            self.drop_index(index_name, table_name)

    def create_index(self, index_query):
        """Creates an index using the specified SQL query."""
        connection = self.connect_to_db()
        cursor = connection.cursor()
        try:
            cursor.execute(index_query)
        except mysql.connector.Error as err:
            print(f"Error creating index: {err}")
        finally:
            cursor.close()
            connection.close()

    def measure_query_times(self, queries):
        """Measures the execution time of each query 30 times and returns the average times."""
        avg_times = []
        for query in queries:
            durations = []
            for _ in range(30):
                duration = self.execute_query(query['sql'], query.get('params'))
                durations.append(duration)
            avg_time = np.nanmean(durations)
            avg_times.append(avg_time)
        return avg_times

    def optimize(self, queries, indices, engines):
        """Optimizes the queries by testing different index combinations and storage engines."""
        all_combinations = list(itertools.chain.from_iterable(
            itertools.combinations(indices, r) for r in range(len(indices)+1)
        ))
        results = []
        for engine in engines:
            self.set_engine(engine)
            self.disable_foreign_keys()
            for combination in tqdm(all_combinations, desc=f"Testing index combinations with engine {engine}"):
                self.drop_indices()
                for index_query in combination:
                    self.create_index(index_query)
                times = self.measure_query_times(queries)
                result = {
                    "Engine": engine,
                    "Indices": combination,
                    "Q1": times[0]
                }
                results.append(result)
            self.enable_foreign_keys()
        return results

    def set_engine(self, engine):
        """Sets the storage engine for the specified tables."""
        tables = ["patient", "sample", "microorganism", "sample_microorganism"]
        for table in tables:
            connection = self.connect_to_db()
            cursor = connection.cursor()
            try:
                cursor.execute(f"ALTER TABLE {table} ENGINE={engine};")
            except mysql.connector.Error as err:
                print(f"Error setting engine {engine} on table {table}: {err}")
            finally:
                cursor.close()
                connection.close()

    def disable_foreign_keys(self):
        """Disables foreign key checks."""
        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        cursor.close()
        connection.close()

    def enable_foreign_keys(self):
        """Enables foreign key checks."""
        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        cursor.close()
        connection.close()

def encode_indices(indices):
    """Encodes a list of index creation SQL queries into their corresponding codes."""
    index_mapping = {
        "CREATE INDEX idx_patient_id ON patient(Patient_ID);": "I1",
        "CREATE INDEX idx_sample_patient_id ON sample(Patient_ID);": "I2",
        "CREATE INDEX idx_sample_id ON sample(Sample_ID);": "I3",
        "CREATE INDEX idx_sample_microorganism_sample_id ON sample_microorganism(Sample_ID);": "I4",
        "CREATE INDEX idx_sample_microorganism_microorganism_id ON sample_microorganism(Microorganism_ID);": "I5",
        "CREATE INDEX idx_sample_microorganism_qpcr ON sample_microorganism(qPCR);": "I6",
        "CREATE INDEX idx_sample_type ON sample(Sample_Type);": "I7"
    }
    encoded_indices = '+'.join(index_mapping[idx] for idx in indices if idx in index_mapping)
    return encoded_indices

def encode_results(df_results):
    """Encodes the indices in the results DataFrame."""
    df_encoded = df_results.copy()
    df_encoded['Indices'] = df_encoded['Indices'].apply(encode_indices)
    return df_encoded

if __name__ == "__main__":
    password = "bdbiO"
    database = "microbiomeDB"

    queries = [
        {"sql": "SELECT p.Patient_ID, COUNT(DISTINCT sm.Microorganism_ID) AS Num_Microorganisms FROM patient p, sample s, sample_microorganism sm WHERE p.Patient_ID= s.Patient_ID AND s.Sample_ID= sm.Sample_ID GROUP BY p.Patient_ID ORDER BY Num_Microorganisms DESC LIMIT 10;"}
    ]

    indices = [
        "CREATE INDEX idx_patient_id ON patient(Patient_ID);",
        "CREATE INDEX idx_sample_patient_id ON sample(Patient_ID);",
        "CREATE INDEX idx_sample_id ON sample(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_sample_id ON sample_microorganism(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_microorganism_id ON sample_microorganism(Microorganism_ID);",
        "CREATE INDEX idx_sample_microorganism_qpcr ON sample_microorganism(qPCR);",
        "CREATE INDEX idx_sample_type ON sample(Sample_Type);"
    ]

    engines = ["MyISAM", "InnoDB", "MEMORY"]

    optimizer = QueryOptimizer(password, database)
    results = optimizer.optimize(queries, indices, engines)

    # Convert results to DataFrame with the specified format
    df_results = pd.DataFrame(results)
    df_results.to_csv("./query_optimization/mysql_optimization_results.csv", index=False)

    # Encode indices and save the new results
    df_encoded_results = encode_results(df_results)
    df_encoded_results.to_csv("./query_optimization/mysql_encoded_optimization_results.csv", index=False)
    print(df_encoded_results)
