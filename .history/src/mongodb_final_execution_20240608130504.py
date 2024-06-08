import time
import csv

class MongoDBFinalExecution:
    def __init__(self, mongo_aggregations: MongoDBAggregations) -> None:
        """
        Initialize with an instance of MongoDBAggregations.
        
        :param mongo_aggregations: Instance of MongoDBAggregations
        """
        self.mongo_aggregations = mongo_aggregations

    def execute_query_multiple_times(self, query_func, *args, **kwargs) -> float:
        """
        Execute a query function multiple times and calculate the average execution time.
        
        :param query_func: The query function to execute
        :param args: Positional arguments for the query function
        :param kwargs: Keyword arguments for the query function
        :return: The average execution time in milliseconds
        """
        execution_times = []
        for _ in range(50):
            start_time = time.time()
            query_func(*args, **kwargs)
            end_time = time.time()
            execution_time_ms = (end_time - start_time) * 1000
            execution_times.append(execution_time_ms)
        
        return sum(execution_times) / len(execution_times)

    def run(self, output_csv: str) -> None:
        """
        Execute all the queries, calculate the average execution time, and save the results to a CSV file.
        
        :param output_csv: The path to the output CSV file
        """
        queries = [
            ("Q1", self.mongo_aggregations.get_patient_with_most_distinct_microorganisms, 'Patients'),
            ("Q2", self.mongo_aggregations.get_samples_and_qpcr_given_microorganism, 'Patients', 'MIC-64254-KRV'),
            ("Q3", self.mongo_aggregations.get_patients_suffering_disease_and_samples, 'Patients', 'Respiratory infections'),
            ("Q4", self.mongo_aggregations.get_number_of_samples_per_type_of_sample, 'Patients'),
            ("Q5", self.mongo_aggregations.get_microorganism_per_sample_type, 'Patients'),
            ("Q6", self.mongo_aggregations.get_patients_diagnosed_with_disease_and_microorganism_disease, 'Patients', 'Hepatitis B', 'Virus'),
            ("Q7", self.mongo_aggregations.get_species_of_microorganism_with_different_sequence, 'Microorganism')
        ]
        
        results = []
        
        for query_name, query_func, *query_args in queries:
            avg_time = self.execute_query_multiple_times(query_func, *query_args)
            results.append({"Query": query_name, "Time_ms": avg_time})
        
        # Save results to CSV
        df = pd.DataFrame(results)
        df.to_csv(output_csv, index=False)

# To use the class:
def main() -> int:
    mongo_uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    db_name = 'BDB2023'
    collection_patients = 'Patients'
    microorganism_collection = 'Microorganism'
    
    mongo = MongoDBAggregations(mongo_uri=mongo_uri, db_name=db_name)
    execution = MongoDBFinalExecution(mongo_aggregations=mongo)
    execution.run(output_csv='query_execution_times.csv')
    
    return 0

if __name__ == "__main__":
    main()
