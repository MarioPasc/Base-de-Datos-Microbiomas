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
    def __init__(self, microorganism_id: str, species: str, kingdom: str, fasta: str, seq_length: int):
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
    def __init__(self, mongo_uri: str, db_name: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]

    def export_to_json(self, patients: List[Patient], output_file: str) -> None:
        data = [patient.to_dict() for patient in patients]
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=4)
            
    def export_to_json_microorganisms(self, microorganisms: List[Microorganism], output_file:str) -> None:
        data = [microorganism.to_dict() for microorganism in microorganisms]
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=4)

    def insert_patients_to_mongodb(self, patients: List[Patient], collection: str) -> None:
        collection_object = self.db[collection]
        data = [patient.to_dict() for patient in patients]
        collection_object.insert_many(data)

    def insert_microorganisms_to_mongodb(self, microorganisms: List[Microorganism], collection: str) -> None:
        collection_object = self.db[collection]
        data = [microorganism.to_dict() for microorganism in microorganisms]
        collection_object.insert_many(data)

class MongoDBAggregations:
    def __init__(self, mongo_uri: str, db_name: str) -> None:
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database(db_name)
        self.db_name = db_name

    def configure_collections(self, collection_microbiome: str, collection_patient: str) -> None:
        collection_microbiome_object = self.db.get_collection(collection_microbiome)
        collection_patient_object = self.db.get_collection(collection_patient)

        pipeline = [
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

        pipeline2 = [
            {
                "$set": {
                    '_id': "$microorganism_id"
                }
            },
            {
                "$unset": 'microorganism_id'
            },
            {
                "$out": {
                    "db": self.db_name,
                    "coll": collection_microbiome
                }
            }
        ]
        collection_microbiome_object.aggregate(pipeline=pipeline2)

    def get_patient_with_most_distinct_microorganisms(self, collection_patient: str) -> Dict[str, Any]:
        collection_patient_object = self.db.get_collection(collection_patient)

        pipeline = [
            {"$unwind": "$samples"},
            {"$unwind": "$samples.microorganisms"},
            {
                "$group": {
                    "_id": "$patient_id",
                    "distinct_microorganisms": {"$addToSet": "$samples.microorganisms.microorganism_id"}
                }
            },
            {
                "$project": {
                    "num_distinct_microorganisms": {"$size": "$distinct_microorganisms"}
                }
            },
            {"$sort": {"num_distinct_microorganisms": DESCENDING}},
            {"$limit": 1}
        ]

        result = list(collection_patient_object.aggregate(pipeline))
        return result[0] if result else {}

    def get_sample_count_by_type(self, collection_patient: str) -> List[Dict[str, Any]]:
        collection_patient_object = self.db.get_collection(collection_patient)

        pipeline = [
            {"$unwind": "$samples"},
            {
                "$group": {
                    "_id": "$samples.sample_type",
                    "amount": {"$addToSet": "$samples.sample_id"}
                }
            },
            {
                "$project": {
                    "sample_type": "$_id",
                    "amount_final": {"$size": "$amount"}
                }
            }
        ]

        result = list(collection_patient_object.aggregate(pipeline))
        return result

    def find_patients_with_hepatitis_b_and_virus(self, collection_patient: str) -> List[Dict[str, Any]]:
        collection_patient_object = self.db.get_collection(collection_patient)

        pipeline = [
            {"$match": {"disease": "Hepatitis B"}},
            {"$unwind": {"path": "$samples"}},
            {"$unwind": {"path": "$samples.microorganisms"}},
            {
                "$lookup": {
                    "from": "Microorganism",
                    "localField": "samples.microorganisms.microorganism_id",
                    "foreignField": "_id",
                    "as": "microorganism_info"
                }
            },
            {"$unwind": {"path": "$microorganism_info"}},
            {"$match": {"microorganism_info.Kingdom": "Virus"}}
        ]

        result = list(collection_patient_object.aggregate(pipeline))
        return result

def main():
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'bdbiO',
        'database': 'microbiomeDB'
    }

    mongo_uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    db_name = 'BDB2023'
    patients_collection = 'Patients'
    microorganism_collection = 'Microorganism'

    extractor = DatabaseExtractor(mysql_config)
    extractor.connect()

    patients = extractor.fetch_patients()
    samples = extractor.fetch_samples()
    microorganisms_sample = extractor.fetch_microorganisms_sample()
    microorganisms = extractor.fetch_microorganisms()

    # Creating a dictionary to quickly access samples by Sample_ID
    samples_dict = {sample.sample_id: sample for sample in samples}

    # Creating a dictionary to quickly access microorganisms by Sample_ID
    microorganisms_dict = {}
    for microorganism in microorganisms_sample:
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

    exporter = JSONExporter(mongo_uri, db_name)
    output_file_patient = './specification-files/patients_data.json'
    output_file_microorganisms = './specification-files/microorganisms.json'
    exporter.export_to_json(patients=patients, output_file=output_file_patient)
    exporter.export_to_json_microorganisms(microorganisms=microorganisms, 
                                           output_file=output_file_microorganisms)
    exporter.insert_patients_to_mongodb(patients, patients_collection)
    exporter.insert_microorganisms_to_mongodb(microorganisms, microorganism_collection)

    # Aggregations
    mongo = MongoDBAggregations(mongo_uri=mongo_uri, db_name=db_name)

    mongo.configure_collections(collection_microbiome=microorganism_collection,
                                collection_patient=patients_collection)

if __name__ == "__main__":
    main()
