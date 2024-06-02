import mysql.connector
import time
import itertools
import pandas as pd

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
        start_time = time.time()
        cursor.execute(query, params)
        cursor.fetchall()  # Fetch results to complete query execution
        duration = time.time() - start_time
        cursor.close()
        connection.close()
        return duration

    def drop_indices(self):
        drop_index_queries = [
            "DROP INDEX IF EXISTS idx_patient_id ON patient;",
            "DROP INDEX IF EXISTS idx_sample_patient_id ON sample;",
            "DROP INDEX IF EXISTS idx_sample_id ON sample;",
            "DROP INDEX IF EXISTS idx_sample_microorganism_sample_id ON sample_microorganism;",
            "DROP INDEX IF EXISTS idx_sample_microorganism_microorganism_id ON sample_microorganism;",
            "DROP INDEX IF EXISTS idx_sample_microorganism_qpcr ON sample_microorganism;",
            "DROP INDEX IF EXISTS idx_patient_disease ON patient;",
            "DROP INDEX IF EXISTS idx_sample_date ON sample;",
            "DROP INDEX IF EXISTS idx_sample_type ON sample;",
            "DROP INDEX IF EXISTS idx_microorganism_species ON microorganism;"
        ]
        connection = self.connect_to_db()
        cursor = connection.cursor()
        for query in drop_index_queries:
            cursor.execute(query)
        cursor.close()
        connection.close()

    def create_index(self, index_query):
        connection = self.connect_to_db()
        cursor = connection.cursor()
        cursor.execute(index_query)
        cursor.close()
        connection.close()

    def measure_query_times(self, queries):
        times = []
        for query in queries:
            duration = self.execute_query(query['sql'], query.get('params'))
            times.append(duration)
        return times

    def optimize(self, queries, indices):
        all_combinations = list(itertools.chain.from_iterable(
            itertools.combinations(indices, r) for r in range(len(indices)+1)
        ))
        results = []
        for combination in all_combinations:
            self.drop_indices()
            for index_query in combination:
                self.create_index(index_query)
            times = self.measure_query_times(queries)
            results.append({
                "indices": combination,
                "times": times
            })
        return results

if __name__ == "__main__":
    password = "bdbiO"
    database = "your_database"

    queries = [
        {"sql": "SELECT p.Patient_ID, COUNT(DISTINCT sm.Microorganism_ID) AS Num_Microorganisms FROM patient p, sample s, sample_microorganism sm WHERE p.Patient_ID= s.Patient_ID AND s.Sample_ID= sm.Sample_ID GROUP BY p.Patient_ID ORDER BY Num_Microorganisms DESC LIMIT 10;"},
        {"sql": "SELECT sm.Sample_ID, sm.qPCR FROM sample_microorganism sm WHERE sm.Microorganism_ID = %s ORDER BY sm.qPCR DESC;", "params": ("microorganism_id",)},
        {"sql": "SELECT p.Patient_ID, s.Sample_ID, s.Date, s.Body_Part, s.Sample_Type FROM patient p, sample s WHERE p.Patient_ID = s.Patient_ID AND p.Disease = %s ORDER BY s.Date;", "params": ("disease",)},
        {"sql": "SELECT s.Sample_Type, COUNT(*) AS Sample_Count FROM sample s GROUP BY s.Sample_Type ORDER BY Sample_Count DESC;"},
        {"sql": "SELECT sm.Microorganism_ID, s.Sample_Type, COUNT(sm.Sample_ID) AS Sample_Count, AVG(sm.qPCR), STDDEV(sm.qPCR) FROM sample s, sample_microorganism sm WHERE s.Sample_ID= sm.Sample_ID GROUP BY s.Sample_Type, sm.Microorganism_ID ORDER BY sm.Microorganism_ID DESC;"},
        {"sql": "SELECT p1.Patient_ID, s1.Max_qPCR FROM (SELECT p.Patient_ID FROM patient p WHERE p.disease='Hepatitis B') p1, (SELECT s.Patient_ID, max(sm.qPCR) as Max_qPCR FROM sample s, microorganism m, sample_microorganism sm WHERE s.Sample_ID=sm.Sample_ID AND sm.Microorganism_ID=m.Microorganism_ID AND m.Species='Hepatitis B Virus' GROUP BY s.Patient_ID) s1 WHERE s1.Patient_ID=p1.Patient_ID;"},
        {"sql": "SELECT Species, COUNT(*) AS Count, AVG(Seq_length) AS avg_SeqLength FROM microorganism GROUP BY Species HAVING Count>1;"}
    ]

    indices = [
        "CREATE INDEX idx_patient_id ON patient(Patient_ID);",
        "CREATE INDEX idx_sample_patient_id ON sample(Patient_ID);",
        "CREATE INDEX idx_sample_id ON sample(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_sample_id ON sample_microorganism(Sample_ID);",
        "CREATE INDEX idx_sample_microorganism_microorganism_id ON sample_microorganism(Microorganism_ID);",
        "CREATE INDEX idx_sample_microorganism_qpcr ON sample_microorganism(qPCR);",
        "CREATE INDEX idx_patient_disease ON patient(Disease);",
        "CREATE INDEX idx_sample_date ON sample(Date);",
        "CREATE INDEX idx_sample_type ON sample(Sample_Type);",
        "CREATE INDEX idx_microorganism_species ON microorganism(Species);"
    ]

    optimizer = QueryOptimizer(password, database)
    results = optimizer.optimize(queries, indices)

    df_results = pd.DataFrame(results)
    df_results.to_csv("optimization_results.csv", index=False)
    print(df_results)
