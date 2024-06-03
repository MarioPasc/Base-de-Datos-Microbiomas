import mysql.connector as mysql

def add_foreign_keys(password: str, database: str):
    """
    Adds foreign keys to the database schema to ensure referential integrity.
    
    Args:
        password (str): The password for the MySQL root user.
        database (str): The name of the database to modify.
    """
    try:
        # Establishing the connection
        connection = mysql.connect(
            host="localhost",
            user="root",
            password=password,
            database=database
        )
        
        cursor = connection.cursor()
        
        # Adding foreign keys
        cursor.execute("""
            ALTER TABLE sample
            ADD CONSTRAINT fk_patient
            FOREIGN KEY (Patient_ID) REFERENCES patient(Patient_ID);
        """)
        
        cursor.execute("""
            ALTER TABLE sample_microorganism
            ADD CONSTRAINT fk_sample
            FOREIGN KEY (Sample_ID) REFERENCES sample(Sample_ID),
            ADD CONSTRAINT fk_microorganism
            FOREIGN KEY (Microorganism_ID) REFERENCES microorganism(Microorganism_ID);
        """)
        
        connection.commit()
        print("Foreign keys added successfully.")
        
    except mysql.Error as err:
        print(f"Error: {err}")
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Call the function to add foreign keys
add_foreign_keys(password="bdbiO", database="microbiomeDB")
