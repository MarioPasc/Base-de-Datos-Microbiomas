import argparse
from mysql_dbCreation import DbCreation

def main():
    parser = argparse.ArgumentParser(description="Connect to a MySQL database using Python.")
    
    parser.add_argument('-p', '--password', 
                        type=str, help='MySQL Password', required=True)
    parser.add_argument('-d', '--database', 
                        type=str, help='Database name', required=True)
    parser.add_argument('-s', '--samples', 
                        type=int, help='Number of rows to insert', required=True)
    args = parser.parse_args()
    
    mydb = mysql_dbCreation.DbCreation(password=args.password, 
                                  database=args.database)
    mydb.insert_data_in_batches(num_samples=args.samples)
    

if __name__ == "__main__":
    main()
