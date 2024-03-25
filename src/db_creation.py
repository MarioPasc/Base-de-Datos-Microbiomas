from typing import List, Optional
import mysql.connector as mysql

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
                    password=self.password)
            cursor= connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        except mysql.Error as err:
            print(f"Error: {err}")
        finally:
            if connection is not None:
                connection.close()
        return connection  