from typing import List, Optional, Dict
import mysql.connector as mysql
import random
import string
import pandas as pd

class DbCreation:
    
    def __init__(self, password: str, database: str) -> None:
        self.password = password
        self.database = database
        # Stablish a connection. If the schema has 
        # already been created, this will have no effect
        self.__connection__()
        
    def __connection__(self):
        try:
            connection = mysql.connect(
                    host="localhost",
                    user="root",
                    password=self.password,
                    database=self.database
                )
            print("Done")
            return connection
        except mysql.Error as err:
            print(f"Error: {err}")
            if 'Unknown database' in str(err):
                try:
                    connection = mysql.connect(
                        host="localhost",
                        user="root",
                        password=self.password
                    )
                    cursor = connection.cursor()
                    cursor.execute(f"CREATE SCHEMA {self.database}")
                    connection.commit()
                    print(f"New schema {self.database} created.")
                    cursor.close()
                except mysql.Error as err:
                    print(f"Error creating schema: {err}")
        finally:
            if connection is not None:
                connection.close()
        return connection
    
    
class DataGenerator:
    def __init__(self, num_samples: int):
        self.num_samples = num_samples
        self.table_names: List[str] = ["Patient", "Sample", "Microorganism", "Sample_Microorganism"]
        self.patient_attributes: List[str] = [
            "Patient_ID",  # String, format: PAC + 5 digits + 3 letters
            "Age",         # Integer, range: [0-100]
            "Birth_Type",  # String, options: ['cesarean', 'natural']
            "Location",    # String, options: ['Europe', 'Africa', 'North America', 'South America', 'Central Asia', 'East Asia']
            "Lifestyle",   # String, options: ['Active', 'Sedentary']
            "Disease"      # String
            ]
        self.sample_attributes: List[str] = [
            "Sample_ID",   # String, format: MUE + 5 digits + 3 letters
            "Patient_ID",  # String, Foreign Key
            "Date",        # String, Date
            "Body_Part",   # String
            "Sample_Type"  # String
            ]
        self.microorganism_attributes: List[str]= [
            "Microorganism_ID", # String, format: MICRO + 5 digits + 3 letters
            "Microorganism_Name",
            "Species",          # String
            "Kingdom",          # String
            "FASTA",            # String, format: seq_ + Microorganism ID + .fasta
            "Seq_length"        # Integer, calculation: 150 * random number [10^6-10^8]
            ]
        self.sample_microorganism_attributes: List[str] = [
            "Sample_ID",        # String, Foreign Key, Primary Key
            "Microorganism_ID", # String, Foreign Key, Primary Key
            "qPCR"              # Integer, Copies/mL, random number
            ]
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


