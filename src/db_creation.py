"""
This Python module is designed to facilitate the generation, management, and simulation of microbiome-related data,
encompassing functionalities for creating mock data sets that mimic real-world microbiome sample data, patient information,
and microorganism characteristics. This data can be used for testing, analysis, and development purposes in bioinformatics
and microbiome research projects.

Documentation within this module adheres to PEP 257 docstring conventions, ensuring clarity and consistency across
descriptions and making the codebase more accessible and maintainable.

Authors:
    - Carmen Rodríguez González
    - Mario Pascual González
    - Gonzalo Mesas Aranda

This code was developed to fulfill an assignment for the final project in the "Biological Data Bases" course.
"""



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
    """
    A class to manage database creation, schema setup, data insertion, and deletion for a microbiome database.

    Attributes:
        password (str): The password to connect to the MySQL database.
        database (str): The name of the database (schema) to be created and managed.

    Methods:
        __init__(self, password: str, database: str): Initializes the database connection and sets up the schema.
        __schema__(self): Establishes a connection to MySQL and creates the database if it does not exist.
        __creation__(self): Creates the necessary tables within the database.
        insert_data_in_batches(self, num_samples: int): Inserts data into the database in batches.
        _insert_dataframe_to_db(self, df, cursor): Inserts data from a DataFrame into the database.
        drop_db(self): Drops the database schema.
    """
    
    def __init__(self, password: str, database: str) -> None:
        """
        Initializes the DbCreation instance, establishes a database connection,
        and sets up the schema and tables if they do not already exist.

        Args:
            password (str): The password for the MySQL database connection.
            database (str): The name of the database (schema) to create and manage.
        """
        self.password = password
        self.database = database
        # Stablish a connection. If the schema has 
        # already been created, this will have no effect
        self.__schema__()
        self.__creation__()
        
    def __schema__(self) -> None:
        """
        Establishes a database connection and creates the database if it does not exist.
        Prints connection status and any errors encountered.
        """
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
        """
        Creates the necessary tables within the database if they do not already exist.
        Defines tables for patients, samples, microorganisms, and sample-microorganism relationships.
        Prints any errors encountered during the table creation process.
        """
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
                    Disease TEXT,
                    Sex ENUM('M', 'F')                          
                )''')
            #char is used cause the ids have fixed length

            cursor.execute('''CREATE TABLE IF NOT EXISTS sample(
                    Sample_ID CHAR(13)  PRIMARY KEY,
                    Patient_ID CHAR(13),       
                    Date DATE,
                    Body_Part VARCHAR(10),
                    Sample_Type VARCHAR(10),
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
        """
        Inserts data into the database in batches. Generates data using the DataGenerator class
        and inserts it in batches for efficiency.

        Args:
            num_samples (int): The total number of samples to generate and insert into the database.
        """
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
        """
        Inserts data from a DataFrame into the database using the provided cursor. Data is inserted
        into the patient, sample, microorganism, and sample_microorganism tables.

        Args:
            df (DataFrame): The DataFrame containing the data to insert.
            cursor (MySQLCursor): The cursor to use for executing SQL queries.
        """
        patient_insert_query = """
        INSERT INTO patient (Patient_ID, Age, Birth_Type, Location, Lifestyle, Disease, Sex) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        Age=VALUES(Age), 
        Birth_Type=VALUES(Birth_Type), 
        Location=VALUES(Location), 
        Lifestyle=VALUES(Lifestyle), 
        Disease=VALUES(Disease),
        Sex=VALUES(Sex)"""

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
            patient_data = (row['Patient_ID'], row['Age'], row['Birth_Type'], row['Location'], row['Lifestyle'], row['Disease'], row['Sex'])
            sample_data = (row['Sample_ID'], row['Patient_ID'], row['Date'], row['Body_Part'], row['Sample_Type'])
            microorganism_data = (row['Microorganism_ID'], row['Species'], row['Kingdom'], row['FASTA'], row['Seq_length'])
            sample_microorganism_data = (row['Microorganism_ID'], row['Sample_ID'], row['qPCR'])
            
            cursor.execute(patient_insert_query, patient_data)
            cursor.execute(sample_insert_query, sample_data)
            cursor.execute(microorganism_insert_query, microorganism_data)
            cursor.execute(sample_microorganism_insert_query, sample_microorganism_data)

    def drop_db(self) -> None:
        """
        Drops the database schema, effectively deleting all data and structures within the database.

        Prints the status of the operation and any errors encountered.
        """
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
    """
    A class for generating random data related to microbiome samples, including microorganism,
    patient, and sample data.

    Attributes:
        num_samples (int): The number of samples to generate.
        seed (int): The seed value for random number generation to ensure reproducibility.

    Methods:
        __init__(self, num_samples: int, seed: int): Initializes the data generator with a specific number of samples and a seed.
        __generateMicroorganismData__(self) -> tuple: Generates random data for microorganisms.
        __generatePatientData__(self) -> list: Generates random data for patients.
        __generateSampleData__(self): Generates random data for samples.
        generate_single_row(self) -> list: Generates a single row of combined data from microorganisms, patients, and samples.
        generate_random_data(self, i: int): Generates random data for the specified number of samples and batches it for insertion.
    """
    
    def __init__(self, num_samples: int, seed: int):
        """
        Initializes the DataGenerator instance with the specified number of samples and seed value.

        Args:
            num_samples (int): The total number of data samples to generate.
            seed (int): The seed value for random number generators to ensure reproducibility.
        """
        self.num_samples = num_samples
        random.seed(seed) # ensure reproducibility
        np.random.seed(seed)
        
        
    def __generateMicroorganismData__(self) -> str:
        """
        Generates random data for a microorganism, including its ID, kingdom, species, and diseases.

        Returns:
            tuple: A tuple containing the microorganism ID, kingdom, species, and diseases.
        """
        csv = pd.read_csv(os.path.join(os.getcwd(), "..", "specification-files", "microorganisms.csv"))
        row = csv.iloc[np.random.randint(0, csv.shape[0]), :]
        return row.loc["Microorganism_ID"], row.loc["Kingdom"], row.loc["Species"], row.loc["Diseases"].split(",")[np.random.randint(0, len(row.loc["Diseases"].split(",")))]
    
    def __generatePatientData__(self) -> List:
        """
        Generates random data for a patient, including ID, age, birth method, location, and activity level.

        Returns:
            list: A list containing the patient's ID, age, birth method, location, and activity level.
        """
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
        sex= ['M','F']

        return [
            patient_id(), # Patient ID
            random.randint(0, 100),  # Age
            random.choice(delivery_methods),  # Birth
            random.choice(locations),  # Localization
            random.choice(activity_levels),  # Activity levels
            random.choice(sex) #Sex
        ]
        
    def __generateSampleData__(self):
        """
        Generates random data for a sample, including sample ID, collection date, body part, and sample type.

        Returns:
            tuple: A tuple containing the sample ID, collection date, body part, and sample type.
        """
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
        """
        Generates a single row of data by combining information from microorganisms, patients, and samples.

        Returns:
            list: A list containing combined data from a microorganism, patient, and sample.
        """
        Microorganism_ID, Kingdom, Species, Diseases = self.__generateMicroorganismData__()
        Fasta = "seq_" + Microorganism_ID + ".fasta"
        Seq_length = np.random.randint(1000000, 100000000)
        qPCR = np.random.randint(50, 1000)
        Patient_ID, Age, Birth, Localization, Activity_Levels, Sex = self.__generatePatientData__()
        Sample_ID, Date, Body_Part, Sample_Type = self.__generateSampleData__()

        return [Patient_ID, Age, Birth, Localization, Activity_Levels, Sex, Diseases, Sample_ID, 
                Date, Body_Part, Sample_Type, Microorganism_ID, Species, Kingdom, Fasta, Seq_length, qPCR]

    def generate_random_data(self, i: int):
        """
        Generates random data for a specified number of samples. Data is generated in parallel
        and combined into a DataFrame.

        Args:
            i (int): An index representing the current batch of data generation, used for tracking in parallel execution.

        Returns:
            pd.DataFrame: A DataFrame containing the generated data for the specified number of samples.
        """
        rows = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.generate_single_row) for _ in range(self.num_samples)]
            for future in tqdm(as_completed(futures), total=self.num_samples, desc=f"Generating Data for batch {i}"):
                rows.append(future.result())

        df = pd.DataFrame(rows, columns=[
            "Patient_ID", "Age", "Birth_Type", "Location", "Lifestyle", "Disease", "Sex","Sample_ID",
            "Date", "Body_Part", "Sample_Type", "Microorganism_ID", "Species", "Kingdom",
            "FASTA", "Seq_length", "qPCR"
        ])

        return df

