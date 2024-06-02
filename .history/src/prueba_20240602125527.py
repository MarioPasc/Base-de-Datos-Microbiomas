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
        times = []
        for query in queries:
            duration = self.execute_query(query['sql'], query.get('params'))
            times.append(duration)
        return times

    def optimize(self, queries, indices, engines):
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
                results.append({
                    "indices": combination,
                    "times": times,
                    "engine": engine
                })
            self.enable_foreign_keys()
        return results

    def set_engine(self, engine):
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
        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        cursor.close()
        connection.close()

    def enable_foreign_keys(self):
        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        cursor.close()
        connection.close()

if __name__ == "__main__":
    password = "bdbiO"
    database = "microbiomeDB"

    queries = [
        {"sql": "SELECT p.Patient_ID, COUNT(DISTINCT sm.Microorganism_ID) AS Num_Microorganisms FROM patient p, sample s, sample_microorganism sm WHERE p.Patient_ID= s.Patient_ID AND s.Sample_ID= sm.Sample_ID GROUP BY p.Patient_ID ORDER BY Num_Microorganisms DESC LIMIT 10;"},
        {"sql": "SELECT sm.Sample_ID, sm.qPCR FROM sample_microorganism sm WHERE sm.Microorganism_ID = %s ORDER BY sm.qPCR DESC;", "params": ("microorganism_id",)},
        {"sql": "SELECT p.Patient_ID, s.Sample_ID, s.Date, s.Body_Part, s.Sample_Type FROM patient p, sample s WHERE p.Patient_ID = s.Patient_ID AND p.Disease = %s ORDER BY s.Date;", "params": ("disease",)},
        {"sql": "SELECT s.Sample_Type, COUNT(*) AS Sample_Count FROM sample s GROUP BY s.Sample_Type ORDER BY Sample_Count DESC;"},
        {"sql": "SELECT sm.Microorganism_ID, s.Sample_Type, COUNT(sm.Sample_ID) AS Sample_Count, AVG(sm.qPCR), STDDEV(sm.qPCR) FROM sample s, sample_microorganism sm WHERE s.Sample_ID= sm.Sample_ID GROUP BY s.Sample_Type, sm.Microorganism_ID ORDER BY sm.Microorganism_ID DESC;"},
        {"sql": "SELECT p1.Patient_ID, s1.Max_qPCR FROM (SELECT p.Patient_ID FROM patient p WHERE p.disease='Hepatitis B') p1, (SELECT s.Patient_ID, max(sm.qPCR) as Max_qPCR FROM sample s, microorganism m, sample_microorganism sm WHERE s.Sample_ID=sm.Sample_ID AND sm.Microorganism_ID=m.Microorganism_ID AND m.Species='Hepatitis B Virus' GROUP BY s.Patient_ID) s1 WHERE s1.Patient_ID=p1.Patient_ID;"},
        {"sql": "SELECT Species, COUNT(*) AS Count, AVG(Seq_length) AS avg_SeqLength FROM microorganism GROUP BY Species HAVING Count>1;"},
        # Large data queries
        {"sql": "SELECT * FROM patient;"},
        {"sql": "SELECT * FROM sample;"},
        {"sql": "SELECT * FROM sample_microorganism;"}
    ]

    indices = [
        "CREATE INDEX idx_patient_id ON patient(Patient_ID);",
        "CREATE INDEX idx_sample_patient_id ON sample(Patient_ID);",
        "CREATE INDEX idx_sample_id ON sample(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_sample_id ON sample_microorganism(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_microorganism_id ON sample_microorganism(Microorganism_ID);",
        "CREATE INDEX idx_sample_microorganism_qpcr ON sample_microorganism(qPCR);",
        "CREATE INDEX idx_patient_disease ON patient(Disease(255));",  # Updated to specify key length
        "CREATE INDEX idx_sample_date ON sample(Date);",
        "CREATE INDEX idx_sample_type ON sample(Sample_Type);",
        "CREATE INDEX idx_microorganism_species ON microorganism(Species(255));"  # Updated to specify key length
    ]

    engines = ["InnoDB", "MyISAM", "MEMORY"]

    optimizer = QueryOptimizer(password, database)
    results = optimizer.optimize(queries, indices, engines)

    df_results = pd.DataFrame(results)
    df_results.to_csv("query_optimization/mysql_optimization_results.csv", index=False)
    print(df_results)
