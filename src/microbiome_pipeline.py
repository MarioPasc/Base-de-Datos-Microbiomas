import argparse
import db_creation

def main():
    parser = argparse.ArgumentParser(description="Connect to a MySQL database using Python.")
    
    parser.add_argument('-p', '--password', 
                        type=str, help='MySQL Password', required=True)
    parser.add_argument('-d', '--database', 
                        type=str, help='Database name', required=True)
    
    args = parser.parse_args()
    
    mydb = db_creation.DbCreation(password=args.password, 
                                  database=args.database)

if __name__ == "__main__":
    main()
