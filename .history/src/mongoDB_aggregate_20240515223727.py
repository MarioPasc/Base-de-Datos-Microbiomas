from typing import List, Dict, Any
import mysql.connector
import json
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import date
import pandas as pd
from tqdm import tqdm

class MongoDBAggregations:
    def __init__(self, mongo_uri: str, db_name: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database(db_name)
        self.db_name = db_name
        
    def get_patient_with_most_distinct_microorganisms(self, collection_patient: str) -> Dict[str, Any]:
        collection_patient_object = self.db.get_collection(collection_patient)
        
        pipeline = [
            # Desanidar muestras y microorganismos
            {"$unwind": "$samples"},
            {"$unwind": "$samples.microorganisms"},
            # Agrupar por patient_id y contar microorganismos distintos
            {
                "$group": {
                    "_id": "$_id",
                    "distinct_microorganisms": {"$addToSet": "$samples.microorganisms.microorganism_id"}
                }
            },
            # Contar la cantidad de microorganismos distintos
            {
                "$project": {
                    "num_distinct_microorganisms": {"$size": "$distinct_microorganisms"}
                }
            },
            # Ordenar por la cantidad de microorganismos distintos en orden descendente
            {"$sort": {"num_distinct_microorganisms": DESCENDING}},
            # Limitar a 1 resultado
            {"$limit": 1}
        ]
        
        result = list(collection_patient_object.aggregate(pipeline))
        return result[0] if result else {}
    
    def get_samples_and_qpcr_given_microorganism(self, collection_patient:str, microorganism_name:str):
        collection_patient_object = self.db.get_collection(collection_patient)
        pipeline = [
                        {
                            '$unwind': {
                                'path': '$samples', 
                                'includeArrayIndex': 'string'
                            }
                        }, {
                            '$unwind': {
                                'path': '$samples.microorganisms', 
                                'includeArrayIndex': 'string'
                            }
                        }, {
                            '$match': {
                                'samples.microorganisms.microorganism_id': microorganism_name
                            }
                        }, {
                            '$project': {
                                'sample': '$samples.sample_id', 
                                'qpcr': '$samples.microorganisms.qpcr'
                            }
                        }
                    ]
        results = list(collection_patient_object.aggregate(pipeline=pipeline))
        return results[0] if results else {}
    
    def get_patients_suffering_disease_and_samples(self, collection_patient:str, disease:str):
        collection_patient_object = self.db[collection_patient]
        pipeline = [
                        {
                            '$match': {
                                'disease': disease
                            }
                        }, {
                            '$project': {
                                'disease': '$disease', 
                                'samples': '$samples'
                            }
                        }, {
                            '$unset': 'samples.microorganisms'
                        }
                    ]
        results = list(collection_patient_object.aggregate(pipeline=pipeline))
        return results[0] if results else {}
        
    def get_number_of_samples_per_type_of_sample(self, collection_patient:str): 
        collection_patient_object = self.db[collection_patient]
        pipeline = [
                        {
                            '$unwind': {
                                'path': '$samples', 
                                'includeArrayIndex': 'string'
                            }
                        }, {
                            '$group': {
                                '_id': '$samples.sample_type', 
                                'sample_count': {
                                    '$sum': 1
                                }
                            }
                        }
                    ]
        results = list(collection_patient_object.aggregate(pipeline=pipeline))
        return results if results else {}
    
    def get_microorganism_per_sample_type(self, collection_patient:str):
        collection_patient_object = self.db.get_collection(collection_patient)
        pipeline = [
                        {"$unwind": "$samples"},
                        {"$unwind": "$samples.microorganisms"},
                        {
                            "$group": {
                                "_id": {
                                    "sample_type": "$samples.sample_type",
                                    "microorganism_id": "$samples.microorganisms.microorganism_id"
                                },
                                "Sample_Count": {"$sum": 1},
                                "Average_qPCR": {"$avg": "$samples.microorganisms.qpcr"},
                                "StdDev_qPCR": {"$stdDevPop": "$samples.microorganisms.qpcr"}
                            }
                        },
                        {
                            "$project": {
                                "microorganism_id": "$_id.microorganism_id",
                                "sample_type": "$_id.sample_type",
                                "Sample_Count": 1,
                                "Average_qPCR": 1,
                                "StdDev_qPCR": 1
                            }
                        },
                        {
                            "$sort": {"microorganism_id": -1}
                        }
                    ]
        result = list(collection_patient_object.aggregate(pipeline))
        return result[0] if result else {}
    
    def get_patients_diagnosed_with_disease_and_microorganism_disease(self, collection_patients:str, disease:str):
        collection_patients_object = self.db[collection_patients]
        pipeline = [
                        {
                            '$match': {
                                'disease': 'Hepatitis B'
                            }
                        }, {
                            '$unwind': {
                                'path': '$samples', 
                                'includeArrayIndex': 'string'
                            }
                        }, {
                            '$unwind': {
                                'path': '$samples.microorganisms', 
                                'includeArrayIndex': 'string'
                            }
                        }, {
                            '$lookup': {
                                'from': 'Microorganism', 
                                'localField': 'samples.microorganisms.microorganism_id', 
                                'foreignField': '_id', 
                                'as': 'microorganism_info'
                            }
                        }, {
                            '$unwind': {
                                'path': '$microorganism_info', 
                                'includeArrayIndex': 'string'
                            }
                        }, {
                            '$match': {
                                'microorganism_info.Kingdom': 'Virus'
                            }
                        }, {
                            '$match': {
                                'microorganism_info.Diseases': 'Hepatitis B'
                            }
                        }, {
                            '$project': {
                                '_id': '$_id', 
                                'patient_disease': '$disease', 
                                'microorganism': '$microorganism_info._id', 
                                'species': '$microorganism_info.Species', 
                                'sample_id': '$samples.sample_id', 
                                'qpcr': '$samples.microorganisms.qpcr'
                            }
                        }
                    ]
        results = list(collection_patients_object.aggregate(pipeline=pipeline))
        return results[:2] if results else {}
    
def main() -> int:
    mongo_uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    db_name = 'BDB2023'
    collection_patients = 'Patients'
    microorganism_collection = 'Microorganism'
    mongo = MongoDBAggregations(mongo_uri=mongo_uri, db_name=db_name)
    
    print("QUERY 1: Patient with the most diverse microbiome \n")
    results = mongo.get_patient_with_most_distinct_microorganisms(collection_patient=collection_patients)
    print(results)
    print("\n")
    print("QUERY 2: Given a microorganism, identify the samples in which it is present and its concentration: \n")
    results = mongo.get_samples_and_qpcr_given_microorganism(collection_patient=collection_patients, 
                                                             microorganism_name='MIC-64254-KRV')
    print(results)
    print("\n")
    print("QUERY 3: Patients who suffer from a certain disease and samples \n")
    results = mongo.get_patients_suffering_disease_and_samples(collection_patient=collection_patients,
                                                               disease='Respiratory infections')
    print(results)
    print("\n")
    print("QUERY 4: Number of samples per type of sample: \n")
    results = mongo.get_number_of_samples_per_type_of_sample(collection_patient=collection_patients)
    print(results)
    print("\n")
    print("QUERY 5: Number of times a microorganism appears in the same sample type: \n")
    results = mongo.get_microorganism_per_sample_type(collection_patient=collection_patients)
    print(results)
    print("\n")
    return 0

if __name__ == "__main__":
    main()