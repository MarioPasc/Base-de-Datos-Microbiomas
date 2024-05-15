from typing import List, Dict, Any
import mysql.connector
import json
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import date
import pandas as pd
from tqdm import tqdm

class Microorganism_Sample:
    def __init__(self, microorganism_id: str, qpcr: int, sample_id: str):
        self.microorganism_id = microorganism_id
        self.qpcr = qpcr
        self.sample_id = sample_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "microorganism_id": self.microorganism_id,
            "qpcr": self.qpcr
        }
        
class Microorganism:
    def __init__(self, microorganism_id: str, species:str, kingdom:str, fasta:str, seq_length:int):
        self.microorganism_id = microorganism_id
        self.species = species
        self.kingdom = kingdom
        self.fasta = fasta
        self.seq_length = seq_length

    def to_dict(self) -> Dict[str, Any]:
        return {
            "microorganism_id": self.microorganism_id,
            "species": self.species,
            "kingdom": self.kingdom,
            "fasta": self.fasta,
            "seq_length": self.seq_length
        }

class Sample:
    def __init__(self, sample_id: str, date: date, body_part: str, sample_type: str, patient_id: str):
        self.sample_id = sample_id
        self.date = date
        self.body_part = body_part
        self.sample_type = sample_type
        self.patient_id = patient_id
        self.microorganisms: List[Microorganism_Sample] = []

    def add_microorganism(self, microorganism: Microorganism_Sample):
        self.microorganisms.append(microorganism)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "date": self.date.isoformat(),  # Convert date to ISO format string
            "body_part": self.body_part,
            "sample_type": self.sample_type,
            "microorganisms": [m.to_dict() for m in self.microorganisms]
        }

class Patient:
    def __init__(self, patient_id: str, age: int, birth_type: str, location: str, lifestyle: str, disease: str, sex: str):
        self.patient_id = patient_id
        self.age = age
        self.birth_type = birth_type
        self.location = location
        self.lifestyle = lifestyle
        self.disease = disease
        self.sex = sex
        self.samples: List[Sample] = []

    def add_sample(self, sample: Sample):
        self.samples.append(sample)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "patient_id": self.patient_id,
            "age": self.age,
            "birth_type": self.birth_type,
            "location": self.location,
            "lifestyle": self.lifestyle,
            "disease": self.disease,
            "sex": self.sex,
            "samples": [s.to_dict() for s in self.samples]
        }

class DatabaseExtractor:
    def __init__(self, mysql_config: Dict[str, str]):
        self.mysql_config = mysql_config
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn = mysql.connector.connect(**self.mysql_config)
        self.cursor = self.conn.cursor(dictionary=True)

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def fetch_patients(self) -> List[Patient]:
        query = "SELECT * FROM patient"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [
            Patient(
                patient_id=row['Patient_ID'],
                age=row['Age'],
                birth_type=row['Birth_Type'],
                location=row['Location'],
                lifestyle=row['Lifestyle'],
                disease=row['Disease'],
                sex=row['Sex']
            )
            for row in rows
        ]

    def fetch_samples(self) -> List[Sample]:
        query = "SELECT * FROM sample"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [
            Sample(
                sample_id=row['Sample_ID'],
                date=row['Date'],
                body_part=row['Body_Part'],
                sample_type=row['Sample_Type'],
                patient_id=row['Patient_ID']
            )
            for row in rows
        ]

    def fetch_microorganisms_sample(self) -> List[Microorganism_Sample]:
        query = "SELECT * FROM sample_microorganism"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [
            Microorganism_Sample(
                microorganism_id=row['Microorganism_ID'],
                qpcr=row['qPCR'],
                sample_id=row['Sample_ID']
            )
            for row in rows
        ]
        
    def fetch_microorganisms(self) -> List[Microorganism]:
        query = "SELECT * FROM microorganism"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [
            Microorganism(
                microorganism_id=row["Microorganism_ID"],
                species=row["Species"],
                kingdom=row["Kingdom"],
                fasta=row["FASTA"],
                seq_length=row["Seq_length"]
            )
            for row in rows
        ]

class JSONExporter:
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def export_to_json(self, patients: List[Patient], output_file: str):
        data = [patient.to_dict() for patient in patients]
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=4)

    def insert_to_mongodb(self, patients: List[Patient]):
        data = [patient.to_dict() for patient in patients]
        self.collection.insert_many(data)

    def insert_csv_to_mongodb(self, csv_file: str, collection_name: str):
        df = pd.read_csv(csv_file)
        data = df.to_dict(orient='records')
        collection = self.db[collection_name]
        collection.insert_many(data)

class MongoDBAggregations:
    def __init__(self, mongo_uri: str, db_name: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database(db_name)
        self.db_name = db_name
        
    def configure_collections(self, collection_microbiome:str, collection_patient:str) -> None:
        collection_microbiome_object = self.db.get_collection(collection_microbiome)
        collection_patient_object = self.db.get_collection(collection_patient)
        
        pipeline =  [
            {
                "$set": {
                    "_id": "$patient_id"
                } 
            },
            {
                "$unset": "patient_id"
            },
            {
                "$out": {
                    "db": self.db_name,
                    "coll": collection_patient
                }
            }
        ]
        collection_patient_object.aggregate(pipeline=pipeline)
                
        collection_patient_object.create_index(
            [('samples.sample_id', ASCENDING)], 
            unique=True
        )
        
        pipeline = [
            {
                "$set": {
                    '_id': '$Microorganism_ID'
                }
            },
            {
                "$unset": 'Microorganism_ID'
            },
            {
                "$out": {
                    "db": self.db_name,
                    "coll": collection_microbiome
                }
            }
        ]
        collection_microbiome_object.aggregate(pipeline=pipeline)
        
    def get_patient_with_most_distinct_microorganisms(self, collection_patient: str) -> Dict[str, Any]:
        collection_patient_object = self.db.get_collection(collection_patient)
        
        pipeline = [
            # Desanidar muestras y microorganismos
            {"$unwind": "$samples"},
            {"$unwind": "$samples.microorganisms"},
            # Agrupar por patient_id y contar microorganismos distintos
            {
                "$group": {
                    "_id": "$patient_id",
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

def main():
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'bdbiO',
        'database': 'microbiomeDB'
    }
    
    mongo_uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    db_name = 'BDB2023'
    collection_name = 'Patients'
    microorganism_collection = 'Microorganism'
    
    
    extractor = DatabaseExtractor(mysql_config)
    extractor.connect()

    patients = extractor.fetch_patients()
    samples = extractor.fetch_samples()
    microorganisms = extractor.fetch_microorganisms_sample()

    # Creating a dictionary to quickly access samples by Sample_ID
    samples_dict = {sample.sample_id: sample for sample in samples}
    
    # Creating a dictionary to quickly access microorganisms by Sample_ID
    microorganisms_dict = {}
    for microorganism in microorganisms:
        if microorganism.sample_id not in microorganisms_dict:
            microorganisms_dict[microorganism.sample_id] = []
        microorganisms_dict[microorganism.sample_id].append(microorganism)

    # Associating microorganisms with samples
    for sample in samples:
        if sample.sample_id in microorganisms_dict:
            for microorganism in microorganisms_dict[sample.sample_id]:
                sample.add_microorganism(microorganism)

    # Associating samples with patients
    for patient in patients:
        for sample in samples:
            if sample.patient_id == patient.patient_id:
                patient.add_sample(sample)

    extractor.disconnect()
    
    exporter = JSONExporter(mongo_uri, db_name, collection_name)
    output_file = './specification-files/patients_data.json'
    exporter.export_to_json(patients, output_file)
    exporter.insert_to_mongodb(patients)

    # Insert CSV data into MongoDB
    csv_file = './specification-files/microorganisms.csv'
    exporter.insert_csv_to_mongodb(csv_file, microorganism_collection)
    
    # Aggregations
    mongo = MongoDBAggregations(mongo_uri=mongo_uri, db_name=db_name)
    
    mongo.configure_collections(collection_microbiome=microorganism_collection,
                                collection_patient=collection_name)


if __name__ == "__main__":
    main()
