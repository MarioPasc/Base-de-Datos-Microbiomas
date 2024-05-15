from typing import List, Dict, Any
import mysql.connector
import json
from pymongo import MongoClient

class Microorganism:
    def __init__(self, microorganism_id: str, qpcr: int):
        self.microorganism_id = microorganism_id
        self.qpcr = qpcr

    def to_dict(self) -> Dict[str, Any]:
        return {
            "microorganism_id": self.microorganism_id,
            "qpcr": self.qpcr
        }

class Sample:
    def __init__(self, sample_id: str, date: str, body_part: str, sample_type: str):
        self.sample_id = sample_id
        self.date = date
        self.body_part = body_part
        self.sample_type = sample_type
        self.microorganisms: List[Microorganism] = []

    def add_microorganism(self, microorganism: Microorganism):
        self.microorganisms.append(microorganism)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "date": self.date,
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
        query = "SELECT * FROM Patient"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [Patient(**row) for row in rows]

    def fetch_samples(self) -> List[Sample]:
        query = "SELECT * FROM Sample"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [Sample(**row) for row in rows]

    def fetch_microorganisms(self) -> List[Microorganism]:
        query = "SELECT * FROM Sample_Microorganism"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [Microorganism(**row) for row in rows]

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

def main():
        mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'bdbiO',
        'database': 'microbiomeDB'
    }
    
    mongo_uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    db_name = 'BDB2023'
    
    migrator = MySQLtoMongoDBMigrator(mysql_config, mongo_uri, db_name)

    extractor = DatabaseExtractor(mysql_config)
    extractor.connect()

    patients = extractor.fetch_patients()
    samples = extractor.fetch_samples()
    microorganisms = extractor.fetch_microorganisms()

    # Creating a dictionary to quickly access samples and microorganisms by ID
    samples_dict = {sample.sample_id: sample for sample in samples}
    microorganisms_dict = {}
    for microorganism in microorganisms:
        if microorganism.sample_id not in microorganisms_dict:
            microorganisms_dict[microorganism.sample_id] = []
        microorganisms_dict[microorganism.sample_id].append(microorganism)

    # Associating samples and microorganisms with patients
    for patient in patients:
        for sample in samples:
            if sample.patient_id == patient.patient_id:
                if sample.sample_id in microorganisms_dict:
                    for microorganism in microorganisms_dict[sample.sample_id]:
                        sample.add_microorganism(microorganism)
                patient.add_sample(sample)

    extractor.disconnect()

    exporter = JSONExporter(mongo_uri, db_name, collection_name)
    output_file = 'patients_data.json'
    exporter.export_to_json(patients, output_file)
    exporter.insert_to_mongodb(patients)

if __name__ == "__main__":
    main()
