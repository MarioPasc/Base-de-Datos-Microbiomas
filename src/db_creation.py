from typing import List
import mysql.connector as mysql
import random
import string
import pandas as pd
import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, timedelta


class DbCreation:
    
    def __init__(self, password: str, database: str) -> None:
        self.password = password
        self.database = database
        # Stablish a connection. If the schema has 
        # already been created, this will have no effect
        self.__schema__()
        self.__creation__()
        
    def __schema__(self) -> None:
        print("Establishing connection...")
        try:
            connection = mysql.connect(
                    host="localhost",
                    user="root",
                    password=self.password)
            cursor= connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if connection is not None:
                connection.close()
    
    def __creation__(self) -> None:
        connection= None
        try:
            connection = mysql.connect(
                    host="localhost",
                    user="root",
                    password=self.password,
                    database= self.database)
            cursor= connection.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS patient (
                    Patient_ID CHAR(13)  PRIMARY KEY,       
                    Age INT CHECK (Age BETWEEN 0 AND 100),
                    Birth_Type ENUM('Cesarean', 'Natural'),
                    Location ENUM('Europe', 'Africa', 'North America', 'South America', 'Central Asia', 'East Asia', 'Antarctica', 'Southeast Asia', 'Middle East', 'Oceania'),
                    Lifestyle ENUM('Active', 'Sedentary'),
                    Disease TEXT                    
                )''')
            #char is used cause the ids have fixed length

            cursor.execute('''CREATE TABLE IF NOT EXISTS sample(
                    Sample_ID CHAR(13)  PRIMARY KEY,
                    Patient_ID CHAR(13),       
                    Date DATE,
                    Body_Part TEXT,
                    Sample_Type TEXT,
                    CONSTRAINT sample_patient FOREIGN KEY (Patient_ID) REFERENCES patient (Patient_ID)                    
                )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS microorganism (
                    Microorganism_ID CHAR(13) PRIMARY KEY,
                    Species TEXT,
                    Kingdom ENUM('Bacteria', 'Fungi', 'Virus', 'Protozoa'),
                    FASTA CHAR(23),
                    Seq_length INT CHECK (Seq_length BETWEEN 1000000 AND 100000000)                
                )''')
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS sample_microorganism (
                    Microorganism_ID CHAR(13),
                    Sample_ID CHAR(13),
                    qPCR INT,
                    PRIMARY KEY( Microorganism_ID, Sample_ID),
                    CONSTRAINT sample_fk FOREIGN KEY (Sample_ID) REFERENCES sample (Sample_ID),
                    CONSTRAINT microog_fk FOREIGN KEY (Microorganism_ID) REFERENCES microorganism (Microorganism_ID)
                
                )''')

        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if connection is not None:
                connection.close() 
     
    def insert_data_in_batches(self, num_samples: int) -> None:
        batch_size = 100
        num_batches = (num_samples + batch_size - 1) // batch_size 

        try:
            connection = mysql.connect(
                host="localhost",
                user="root",
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()

            seed = 42
            i = 0
            for _ in tqdm(range(num_batches), desc="Inserting batches"):
                datagen = DataGenerator(num_samples=batch_size,
                                        seed=seed)
                df = datagen.generate_random_data(i=i)  
                self._insert_dataframe_to_db(df, cursor)  
                connection.commit()
                seed += 1
                i += 1

        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()             
    
    def _insert_dataframe_to_db(self, df, cursor):
        patient_insert_query = """
        INSERT INTO patient (Patient_ID, Age, Birth_Type, Location, Lifestyle, Disease) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        Age=VALUES(Age), 
        Birth_Type=VALUES(Birth_Type), 
        Location=VALUES(Location), 
        Lifestyle=VALUES(Lifestyle), 
        Disease=VALUES(Disease)"""

        sample_insert_query = """
        INSERT INTO sample (Sample_ID, Patient_ID, Date, Body_Part, Sample_Type) 
        VALUES (%s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        Date=VALUES(Date), 
        Body_Part=VALUES(Body_Part), 
        Sample_Type=VALUES(Sample_Type)"""

        microorganism_insert_query = """
        INSERT INTO microorganism (Microorganism_ID, Species, Kingdom, FASTA, Seq_length) 
        VALUES (%s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        Species=VALUES(Species), 
        Kingdom=VALUES(Kingdom), 
        FASTA=VALUES(FASTA), 
        Seq_length=VALUES(Seq_length)"""

        sample_microorganism_insert_query = """
        INSERT INTO sample_microorganism (Microorganism_ID, Sample_ID, qPCR) 
        VALUES (%s, %s, %s) 
        ON DUPLICATE KEY UPDATE qPCR=VALUES(qPCR)"""

        for _, row in df.iterrows():
            patient_data = (row['Patient_ID'], row['Age'], row['Birth_Type'], row['Location'], row['Lifestyle'], row['Disease'])
            sample_data = (row['Sample_ID'], row['Patient_ID'], row['Date'], row['Body_Part'], row['Sample_Type'])
            microorganism_data = (row['Microorganism_ID'], row['Species'], row['Kingdom'], row['FASTA'], row['Seq_length'])
            sample_microorganism_data = (row['Microorganism_ID'], row['Sample_ID'], row['qPCR'])
            
            cursor.execute(patient_insert_query, patient_data)
            cursor.execute(sample_insert_query, sample_data)
            cursor.execute(microorganism_insert_query, microorganism_data)
            cursor.execute(sample_microorganism_insert_query, sample_microorganism_data)

    def drop_db(self) -> None:
        try:
            connection = mysql.connect(
                    host="localhost",
                    user="root",
                    database = self.database,
                    password=self.password)
            cursor = connection.cursor()
            cursor.execute(f"DROP SCHEMA IF EXISTS {self.database}")
            print(f"Schema {self.database} dropped.")
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()        
            
class DataGenerator:
    def __init__(self, num_samples: int, seed: int):
        self.num_samples = num_samples
        random.seed(seed) # ensure reproducibility
        np.random.seed(seed)
        
        
    def __generateMicroorganismData__(self) -> str:
        csv = pd.read_csv(os.path.join(os.getcwd(), "..", "specification-files", "microorganisms.csv"))
        row = csv.iloc[np.random.randint(0, csv.shape[0]), :]
        return row.loc["Microorganism_ID"], row.loc["Kingdom"], row.loc["Species"], row.loc["Diseases"].split(",")[np.random.randint(0, len(row.loc["Diseases"].split(",")))]
    
    def __generatePatientData__(self) -> List:
        def patient_id():
            numbers = ''.join(random.choices(string.digits, k=5))
            letters = ''.join(random.choices(string.ascii_uppercase, k=3))
            return f'PAC-{numbers}-{letters}'

        delivery_methods = ['Cesarean', 'Natural']
        locations = [
            'Europe', 'Africa', 'North America', 'South America', 
            'Central Asia', 'East Asia', 'Antarctica', 
            'Southeast Asia', 'Middle East', 'Oceania'
        ]
        activity_levels = ['Active', 'Sedentary']

        return [
            patient_id(), # Patient ID
            random.randint(0, 100),  # Age
            random.choice(delivery_methods),  # Birth
            random.choice(locations),  # Localization
            random.choice(activity_levels)  # Activity levels
        ]
        
    def __generateSampleData__(self):
        def sample_id():
            numbers = ''.join(random.choices(string.digits, k=5))
            letters = ''.join(random.choices(string.ascii_uppercase, k=3))
            return f'SMP-{numbers}-{letters}'

        def random_date(start, end):
            delta = end - start
            int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
            random_second = random.randrange(int_delta)
            return start + timedelta(seconds=random_second)

        body_parts = ['Head', 'Chest', 'Arm', 'Leg', 'Foot', 'Hand']
        sample_types = ['Blood', 'Tissue', 'Saliva', 'Urine']

        date = random_date(datetime(2002, 1, 1), datetime(2023, 12, 31)).strftime('%Y-%m-%d')
        body_part = random.choice(body_parts)
        sample_type = random.choice(sample_types)

        return sample_id(), date, body_part, sample_type   
        
    
    def generate_single_row(self) -> List:
        Microorganism_ID, Kingdom, Species, Diseases = self.__generateMicroorganismData__()
        Fasta = "seq_" + Microorganism_ID + ".fasta"
        Seq_length = np.random.randint(1000000, 100000000)
        qPCR = np.random.randint(50, 1000)
        Patient_ID, Age, Birth, Localization, Activity_Levels = self.__generatePatientData__()
        Sample_ID, Date, Body_Part, Sample_Type = self.__generateSampleData__()

        return [Patient_ID, Age, Birth, Localization, Activity_Levels, Diseases, Sample_ID, 
                Date, Body_Part, Sample_Type, Microorganism_ID, Species, Kingdom, Fasta, Seq_length, qPCR]

    def generate_random_data(self, i: int):
        rows = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.generate_single_row) for _ in range(self.num_samples)]
            for future in tqdm(as_completed(futures), total=self.num_samples, desc=f"Generating Data for batch {i}"):
                rows.append(future.result())

        df = pd.DataFrame(rows, columns=[
            "Patient_ID", "Age", "Birth_Type", "Location", "Lifestyle", "Disease", "Sample_ID",
            "Date", "Body_Part", "Sample_Type", "Microorganism_ID", "Species", "Kingdom",
            "FASTA", "Seq_length", "qPCR"
        ])

        return df

