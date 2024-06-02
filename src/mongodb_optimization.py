from pymongo import MongoClient, ASCENDING
import time
import itertools
import pandas as pd
from tqdm import tqdm
from src.mongodb_queries import MongoDBAggregations

def drop_indexes(db, collection_name):
    collection = db[collection_name]
    indexes = collection.index_information()
    for index in indexes:
        if index != '_id_':
            collection.drop_index(index)
    print(f"All indexes dropped for collection {collection_name}")

def create_index(db, collection_name, index):
    collection = db[collection_name]
    collection.create_index(index)
    print(f"Index created: {index}")

def measure_query_times(mongo_aggregations, collection_patients, collection_microorganism):
    queries = [
        ("Patient with the most diverse microbiome", mongo_aggregations.get_patient_with_most_distinct_microorganisms, collection_patients),
        ("Given a microorganism, identify the samples in which it is present and its concentration", mongo_aggregations.get_samples_and_qpcr_given_microorganism, collection_patients, 'MIC-64254-KRV'),
        ("Patients who suffer from a certain disease and samples", mongo_aggregations.get_patients_suffering_disease_and_samples, collection_patients, 'Respiratory infections'),
        ("Number of samples per type of sample", mongo_aggregations.get_number_of_samples_per_type_of_sample, collection_patients),
        ("Number of times a microorganism appears in the same sample type", mongo_aggregations.get_microorganism_per_sample_type, collection_patients),
        ("Patients who suffer from and have been diagnosed with a disease and have that disease microorganism", mongo_aggregations.get_patients_diagnosed_with_disease_and_microorganism_disease, collection_patients, 'Hepatitis B', 'Virus'),
        ("Find species of microorganism with different sequence registered", mongo_aggregations.get_species_of_microorganism_with_different_sequence, collection_microorganism)
    ]
    
    times = {}
    for description, func, *args in queries:
        start_time = time.time()
        func(*args)
        end_time = time.time()
        duration = end_time - start_time
        times[description] = duration
        print(f"Query: {description}\nTime: {duration:.4f} seconds\n")
    return times

def main():
    mongo_uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    db_name = 'BDB2023'
    collection_patients = 'Patients'
    collection_microorganism = 'Microorganism'
    mongo = MongoDBAggregations(mongo_uri=mongo_uri, db_name=db_name)
    
    client = MongoClient(mongo_uri)
    db = client[db_name]
    
    # Define indices
    indices = [
        ('samples.sample_id', ASCENDING),
        ('samples.microorganisms.microorganism_id', ASCENDING),
        ('disease', ASCENDING),
        ('samples.sample_type', ASCENDING),
        ('microorganism_id', ASCENDING)
    ]
    
    # Generate all combinations of indices
    index_combinations = []
    for i in range(1, len(indices) + 1):
        for combination in itertools.combinations(indices, i):
            index_combinations.append(combination)
    
    # Add the combination with no indices
    index_combinations.insert(0, [])
    
    # DataFrame to store results
    query_names = [
        "Patient with the most diverse microbiome",
        "Given a microorganism, identify the samples in which it is present and its concentration",
        "Patients who suffer from a certain disease and samples",
        "Number of samples per type of sample",
        "Number of times a microorganism appears in the same sample type",
        "Patients who suffer from and have been diagnosed with a disease and have that disease microorganism",
        "Find species of microorganism with different sequence registered"
    ]
    
    results_df = pd.DataFrame(index=query_names, columns=['No Indices'] + [str(combination) for combination in index_combinations[1:]])
    
    for combination in tqdm(index_combinations, desc="Index Combinations"):
        combination_name = 'No Indices' if not combination else str(combination)
        print(f"\nEvaluating combination: {combination_name}")
        
        # Drop all indexes first
        print("Dropping indexes...")
        drop_indexes(db, collection_patients)
        drop_indexes(db, collection_microorganism)
        
        # Create indexes for the current combination
        if combination:
            for index in combination:
                create_index(db, collection_patients, [index])
        
        # Measure query times
        print("Measuring query times...")
        times = measure_query_times(mongo, collection_patients, collection_microorganism)
        
        # Save times in DataFrame
        for query, time_taken in times.items():
            results_df.at[query, combination_name] = time_taken
    
    # Save DataFrame to CSV
    results_df.to_csv('query_performance.csv')
    print("Results saved to query_performance.csv")

if __name__ == "__main__":
    main()
