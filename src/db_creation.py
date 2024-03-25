from typing import List, Optional
import mysql.connector as mysql

class DbCreation:
    
    def __init__(self, password: str, database: str) -> None:
        self.password = password
        self.database = database
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
                    
        
def main():
    mydb = DbCreation(password="st1rch*bdbiO",
                      database="microbioma_db")      
    
if __name__ == "__main__":
    main()      