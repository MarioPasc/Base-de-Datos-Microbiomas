import argparse
import mysql_queries as mysql_queries


def main():
    parser = argparse.ArgumentParser(description="Connect to a MySQL database using Python.")
    
    parser.add_argument('-p', '--password', 
                        type=str, help='MySQL Password', required=True)
    parser.add_argument('-d', '--database', 
                        type=str, help='Database name', required=True)
    args = parser.parse_args()
    
    myqueries = mysql_queries.Queries(password=args.password, 
                                  database=args.database)
    myqueries.__query1__()
    myqueries.__query2__("MIC-17098-ZUZ") #enter the desire microorganism ID
    myqueries.__query3__("Tuberculosis") #enter a disease
    myqueries.__query4__()
    myqueries.__query5__()
    myqueries.__query6__()
    myqueries.__query7__()

if __name__ == "__main__":
    main()