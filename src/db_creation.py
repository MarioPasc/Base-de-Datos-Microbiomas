from typing import List, Optional
import mysql.connector as mysql

class DbCreation:
    
    def __init__(self, password: str, database: str) -> None:
        self.password = password
        self.database = database
        # Stablish a connection. If the schema has 
        # already been created, this will have no effect
        self.__connection__()
        self.__creation__()
        
    def __connection__(self):
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
                    Patient_ID CHAR(11)  PRIMARY KEY,       
                    Age INT CHECK (Age BETWEEN 0 AND 100),
                    Birth_Type ENUM('cesarean', 'natural'),
                    Location ENUM('Europe', 'Africa', 'North America', 'South America', 'Central Asia', 'East Asia'),
                    Lifestyle ENUM('Active', 'Sedentary'),
                    Disease TEXT                    
                )''')
            #char is used cause the ids have fixed length

            cursor.execute('''CREATE TABLE IF NOT EXISTS sample(
                    Sample_ID CHAR(11)  PRIMARY KEY,
                    Patient_ID CHAR(11),       
                    Date DATE,
                    Body_Part TEXT,
                    Sample_Type TEXT,
                    CONSTRAINT sample_patient FOREIGN KEY (Patient_ID) REFERENCES patient (Patient_ID)                    
                )''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS microorganism (
                        Microorganism_ID CHAR(13) PRIMARY KEY,
                        Microorganism_Name TEXT,
                        Species TEXT,
                        Kingdom ENUM('bacterium', 'fungi', 'virus', 'protist', 'archaea'),
                        FASTA CHAR(23),
                        Seq_length INT CHECK (Seq_length BETWEEN 1000000 AND 100000000)                
                )''')
            
            cursor.execute('''CREATE TABLE IF NOT EXISTS sample_microorganism (
                        Microorganism_ID CHAR(13),
                        Sample_ID CHAR(11),
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

