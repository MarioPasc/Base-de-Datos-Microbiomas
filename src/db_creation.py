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
        self.__connection__()
        self.__creation__()
        
    def __connection__(self):
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
    
    def __creation__(self):
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
                    Birth_Type ENUM('cesarean', 'natural'),
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
                    Kingdom ENUM('bacterium', 'fungi', 'virus', 'protist', 'archaea'),
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
    
    
class DataGenerator:
    def __init__(self, num_samples: int):
        self.num_samples = num_samples
        random.seed(42) # ensure reproducibility
        np.random.seed(42)
        self.table_names: List[str] = ["Patient", "Sample", "Microorganism", "Sample_Microorganism"]
        self.all_attributes: List[str] = [
            "Patient_ID",
            "Age",
            "Birth_Type",
            "Location",
            "Lifestyle",
            "Disease",
            "Sample_ID",
            "Date",
            "Body_Part",
            "Sample_Type",
            "Microorganism_Name"
            "Microorganism_ID",
            "Species",
            "Kingdom",
            "FASTA",
            "Seq_length",
            "qPCR"
            ]
        
        
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
        # Genera una sola fila de datos.
        Microorganism_ID, Kingdom, Species, Diseases = self.__generateMicroorganismData__()
        Fasta = "seq_" + Microorganism_ID + ".fasta"
        Seq_length = np.random.randint(1000000, 100000000)
        qPCR = np.random.randint(50, 1000)
        Patient_ID, Age, Birth, Localization, Activity_Levels = self.__generatePatientData__()
        Sample_ID, Date, Body_Part, Sample_Type = self.__generateSampleData__()

        return [Patient_ID, Age, Birth, Localization, Activity_Levels, Diseases, Sample_ID, 
                Date, Body_Part, Sample_Type, Microorganism_ID, Species, Kingdom, Fasta, Seq_length, qPCR]

    def generate_random_data(self):
        rows = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.generate_single_row) for _ in range(self.num_samples)]
            for future in tqdm(as_completed(futures), total=self.num_samples, desc="Generating Data"):
                rows.append(future.result())

        df = pd.DataFrame(rows, columns=[
            "Patient_ID", "Age", "Birth_Type", "Location", "Lifestyle", "Disease", "Sample_ID",
            "Date", "Body_Part", "Sample_Type", "Microorganism_ID", "Species", "Kingdom",
            "FASTA", "Seq_length", "qPCR"
        ])

        return df

