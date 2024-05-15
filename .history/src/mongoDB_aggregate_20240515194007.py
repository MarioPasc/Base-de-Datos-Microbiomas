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

    
    
    
def main() -> int:
    mongo_uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    db_name = 'BDB2023'
    collection_patients = 'Patients'
    microorganism_collection = 'Microorganism'
    mongo = MongoDBAggregations(mongo_uri=mongo_uri, db_name=db_name)
    
    results = mongo.get_patient_with_most_distinct_microorganisms(collection_patient=collection_patients)
    print(results)
    results = mongo.get_microorganism_per_sample_type(collection_patient=collection_patients)
    print(results)
    return 0

if __name__ == "__main__":
    main()