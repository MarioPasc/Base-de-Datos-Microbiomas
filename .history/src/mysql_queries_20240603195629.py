import os
import time
import mysql.connector as mysql
import csv

import pandas as pd

class Queries:
    def __init__(self, password: str, database: str) -> None:
        self.password = password
        self.database = database


    def __connection__(self):
        try:
            connection = mysql.connect(
                    host="localhost",
                    user="root",
                    password=self.password,
                    database= self.database)
            return  connection
        except mysql.Error as err:
            print(f"Error: {err}")


    def __query_format__(self, query: str, argument: tuple, num_qury: int):
        connection= self.__connection__()
        cursor=connection.cursor()
        try:
            start_time= time.time()
            cursor.execute(query, argument)
            result= cursor.fetchall()
            final_time = time.time() - start_time
            col_name= [d[0] for d in cursor.description]
            #print("This query as take ",final_time, " s")
            self.__export_csv__(result,col_name, "query"+str(num_qury)+".csv")
            return final_time
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def query1(self):
        # Find the 10 patients with the most diverse global microbiomes
        query='''SELECT p.Patient_ID, COUNT(DISTINCT sm.Microorganism_ID) AS Num_Microorganisms
                            FROM patient p, sample s, sample_microorganism sm
                            WHERE p.Patient_ID= s.Patient_ID AND s.Sample_ID= sm.Sample_ID
                            GROUP BY p.Patient_ID
                            ORDER BY Num_Microorganisms DESC
                            LIMIT 10;'''
        return self.__query_format__(query,(),1)


    def query2(self, microorganism_ID: str):
        #Identify the sample and its qPCR for a given Microorganism

        query='''SELECT sm.Sample_ID, sm.qPCR
                            FROM sample_microorganism sm
                            WHERE sm.Microorganism_ID = %s 
                            ORDER BY sm.qPCR DESC;'''
        return self.__query_format__(query, (microorganism_ID,),2)
        
    

    def query3(self, disease: str):
        #List the patients who suffer a certain disease and their associated samples.
        query='''SELECT p.Patient_ID, s.Sample_ID, s.Date, s.Body_Part, s.Sample_Type
                            FROM patient p, sample s 
                            WHERE  p.Patient_ID = s.Patient_ID AND p.Disease = %s
                            ORDER BY s.Date;'''
        return self.__query_format__(query,(disease,),3)


    def query4(self):
        #Number of Samples per type of sample.
        query='''SELECT s.Sample_Type, COUNT(*) AS Sample_Count
                            FROM sample s
                            GROUP BY s.Sample_Type
                            ORDER BY Sample_Count DESC;'''
        return self.__query_format__(query,(),4)
    
    def query5(self):
        #number of times a microorganism appears in the same sample type 

        query='''SELECT sm.Microorganism_ID, s.Sample_Type, COUNT(sm.Sample_ID) AS Sample_Count, AVG(sm.qPCR), STDDEV(sm.qPCR)
                FROM sample s, sample_microorganism sm
                WHERE s.Sample_ID= sm.Sample_ID
                GROUP BY s.Sample_Type, sm.Microorganism_ID
                ORDER BY sm.Microorganism_ID DESC;'''
        return self.__query_format__(query,(),5)
 
    def query6(self):
        #Find patients who suffer from and have been diagnosed with hepatitis B and have the hepatitis B virus in their microbiome and the qPCR of this microorganism
        query='''SELECT p1.Patient_ID, s1.Max_qPCR
                FROM (SELECT p.Patient_ID
		                FROM patient p
		                WHERE p.disease='Hepatitis B') p1, (SELECT s.Patient_ID, max(sm.qPCR) as Max_qPCR
											                FROM sample s, microorganism m, sample_microorganism sm
											                WHERE s.Sample_ID=sm.Sample_ID AND 
                                                                    sm.Microorganism_ID=m.Microorganism_ID AND 
                                                                    m.Species='Hepatitis B Virus'
                                                            GROUP BY s.Patient_ID) s1
                WHERE s1.Patient_ID=p1.Patient_ID;'''
        return self.__query_format__(query,(),6)

    def query7(self):
        # Find microorganism of the same species with different sequence length.
        query='''SELECT Species, COUNT(*) AS Count, AVG(Seq_length) AS avg_SeqLength
                FROM microorganism
                GROUP BY Species
                HAVING Count>1;'''
        return self.__query_format__(query,(),7)


    def __export_csv__(self, result,col_name, file_name: str):
        file_path = os.path.join(os.getcwd(),"csv-queries", file_name)
        df = pd.DataFrame.from_records(result, columns=col_name)
        df.to_csv(file_path, index=False)


        
     
        
    