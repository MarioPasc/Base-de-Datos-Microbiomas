from typing import List, Dict, Any
import mysql.connector
import json
from pymongo import MongoClient
from datetime import date
import pandas as pd


class Microorganism:
    def __init__(self, microorganism_id: str, qpcr: int, sample_id: str):
        self.microorganism_id = microorganism_id
        self.qpcr = qpcr
        self.sample_id = sample_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "microorganism_id": self.microorganism_id,
            "qpcr": self.qpcr
        }

class Sample:
    def __init__(self, sample_id: str, date: date, body_part: str, sample_type: str, patient_id: str):
        self.sample_id = sample_id
        self.date = date
        self.body_part = body_part
        self.sample_type = sample_type
        self.patient_id = patient_id
        self.microorganisms: List[Microorganism] = []

    def add_microorganism(self, microorganism: Microorganism):
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

    def fetch_microorganisms(self) -> List[Microorganism]:
        query = "SELECT * FROM sample_microorganism"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [
            Microorganism(
                microorganism_id=row['Microorganism_ID'],
                qpcr=row['qPCR'],
                sample_id=row['Sample_ID']
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
    microorganisms = extractor.fetch_microorganisms()

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
    output_file = 'patients_data.json'
    exporter.export_to_json(patients, output_file)
    exporter.insert_to_mongodb(patients)

    # Insert CSV data into MongoDB
    csv_file = './specification-files/microorganisms.csv'
    exporter.insert_csv_to_mongodb(csv_file, microorganism_collection)

if __name__ == "__main__":
    main()
