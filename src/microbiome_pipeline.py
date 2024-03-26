import argparse
import db_creation
import pandas
import os

def main():
    parser = argparse.ArgumentParser(description="Connect to a MySQL database using Python.")
    
    parser.add_argument('-p', '--password', 
                        type=str, help='MySQL Password', required=True)
    parser.add_argument('-d', '--database', 
                        type=str, help='Database name', required=True)
    parser.add_argument('-s', '--samples', 
                        type=int, help='Number of rows to insert', required=True)
    args = parser.parse_args()
    
    mydb = db_creation.DbCreation(password=args.password, 
                                  database=args.database)
    
    datagen = db_creation.DataGenerator(num_samples=args.samples)
    dataframe_data = datagen.generate_random_data()
    dataframe_data.to_csv(os.path.join(os.getcwd(), "..", "specification-files", "randomData.csv"), index=False)

if __name__ == "__main__":
    main()
