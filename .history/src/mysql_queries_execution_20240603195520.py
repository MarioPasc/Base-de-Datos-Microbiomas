from mysql_queries import Queries


def main():
    
    myqueries = Queries(password="bdbiO", 
                                  database="microbiomeDB")
    myqueries.__query1__()
    myqueries.__query2__("MIC-17098-ZUZ") #enter the desire microorganism ID
    myqueries.__query3__("Tuberculosis") #enter a disease
    myqueries.__query4__()
    myqueries.__query5__()
    myqueries.__query6__()
    myqueries.__query7__()

if __name__ == "__main__":
    main()