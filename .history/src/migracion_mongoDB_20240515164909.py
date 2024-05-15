from typing import List, Dict, Any
import mysql.connector
import json
import pymongo
from dataclasses import dataclass, asdict

@dataclass
class Microorganism:
    id: str
    qPCR: float

@dataclass
class Sample:
    sample_id: str
    date: str
    body_part: str
    sample_type: str
    microorganisms: List[Microorganism]

@dataclass
class Patient:
    patient_id: str
    age: int
    birth_type: str
    location: str
    lifestyle: str
    disease: str
    sex: str
    samples: List[Sample]

class MySQLtoMongoDBMigrator:
    def __init__(self, mysql_config: Dict[str, Any], mongo_uri: str, db_name: str):
        self.mysql_config = mysql_config
        self.mongo_uri = mongo_uri
        self.db_name = db_name

    def fetch_data_from_mysql(self) -> List[Patient]:
        conn = mysql.connector.connect(**self.mysql_config)
        cursor = conn.cursor(dictionary=True)
        
        # Fetch patients
        cursor.execute("SELECT * FROM Patient")
        patients_data = cursor.fetchall()
        
        # Fetch samples
        cursor.execute("SELECT * FROM Sample")
        samples_data = cursor.fetchall()
        
        # Fetch microorganisms (assuming a table named 'Microorganism')
        cursor.execute("SELECT * FROM Microorganism")
        microorganisms_data = cursor.fetchall()
        
        # Organize data
        patients_dict = {patient['Patient_ID']: patient for patient in patients_data}
        samples_dict = {patient_id: [] for patient_id in patients_dict.keys()}
        
        for sample in samples_data:
            samples_dict[sample['Patient_ID']].append(sample)
        
        microorganism_dict = {sample_id: [] for sample_id in [s['Sample_ID'] for s in samples_data]}
        
        for microorganism in microorganisms_data:
            microorganism_dict[microorganism['Sample_ID']].append(microorganism)
        
        # Create patient objects
        patients = []
        for patient_id, patient_data in patients_dict.items():
            samples = []
            for sample_data in samples_dict[patient_id]:
                microorganisms = [Microorganism(m['Microorganism_ID'], m['qPCR']) for m in microorganism_dict[sample_data['Sample_ID']]]
                sample = Sample(
                    sample_id=sample_data['Sample_ID'],
                    date=sample_data['Date'],
                    body_part=sample_data['Body_Part'],
                    sample_type=sample_data['Sample_Type'],
                    microorganisms=microorganisms
                )
                samples.append(sample)
            patient = Patient(
                patient_id=patient_data['Patient_ID'],
                age=patient_data['Age'],
                birth_type=patient_data['Birth_Type'],
                location=patient_data['Location'],
                lifestyle=patient_data['Lifestyle'],
                disease=patient_data['Disease'],
                sex=patient_data['Sex'],
                samples=samples
            )
            patients.append(patient)
        
        cursor.close()
        conn.close()
        return patients

    def insert_data_into_mongodb(self, patients: List[Patient]):
        client = pymongo.MongoClient(self.mongo_uri)
        db = client[self.db_name]
        collection = db['Patients']
        
        # Convert patient objects to dictionaries and insert into MongoDB
        patients_dicts = [asdict(patient) for patient in patients]
        collection.insert_many(patients_dicts)
        
        client.close()

    def migrate(self):
        patients = self.fetch_data_from_mysql()
        self.insert_data_into_mongodb(patients)
        
if __name__ == "__main__":
    uri = "mongodb+srv://pascualgonzalezmario:admin@cluster0.emhrxxc.mongodb.net/"
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'bdbiO',
        'database': ''
    }
    
    mongo_uri = ''
    db_name = 'BDB2023'
    
    migrator = MySQLtoMongoDBMigrator(mysql_config, mongo_uri, db_name)
    migrator.migrate()
